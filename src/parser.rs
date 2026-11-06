use anyhow::{Context, Result, bail, ensure};
use calamine::{Data, Range, Reader, open_workbook_auto};
use serde::Serialize;
use std::{
    collections::{BTreeMap, HashSet},
    io::Read,
    path::Path,
};
use tempfile::NamedTempFile;
use tokio::{sync::mpsc, task::JoinHandle};

pub type Row = Vec<Option<String>>;
#[derive(Clone, Copy, Debug)]
pub enum Kind {
    Csv,
    TrialBalance,
}
#[derive(Debug)]
pub enum Event {
    Header(Vec<String>),
    Rows(Vec<Row>),
    Metadata { columns: Vec<String>, row: Row },
    Done(u64),
}

pub struct ParseStream {
    pub events: mpsc::Receiver<Event>,
    pub task: JoinHandle<Result<()>>,
}

/// Own the temporary file until the blocking parser finishes, including on cancellation.
pub fn spawn(file: NamedTempFile, kind: Kind, batch_rows: usize, queue: usize) -> ParseStream {
    let (sender, events) = mpsc::channel(queue);
    let task = tokio::task::spawn_blocking(move || match kind {
        Kind::Csv => parse_csv(std::fs::File::open(file.path())?, batch_rows, &sender),
        Kind::TrialBalance => parse_trial_balance(file.path(), batch_rows, &sender),
    });
    ParseStream { events, task }
}

fn send(sender: &mpsc::Sender<Event>, event: Event) -> Result<()> {
    sender
        .blocking_send(event)
        .context("Parser consumer stopped")
}

pub fn validate_headers(headers: &[String]) -> Result<()> {
    ensure!(!headers.is_empty(), "CSV has no columns");
    let mut seen = HashSet::new();
    for header in headers {
        ensure!(
            !header.is_empty() && header.chars().count() <= 128,
            "Empty or overlong CSV column name"
        );
        ensure!(
            seen.insert(header.to_ascii_lowercase()),
            "Duplicate CSV header: {header}"
        );
        ensure!(
            !header.to_ascii_lowercase().starts_with("x_"),
            "Source header conflicts with loader columns: {header}"
        );
    }
    Ok(())
}

fn parse_csv(reader: impl Read, batch_rows: usize, sender: &mpsc::Sender<Event>) -> Result<()> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(false)
        .from_reader(reader);
    let headers: Vec<String> = reader
        .headers()?
        .iter()
        .enumerate()
        .map(|(i, s)| {
            if i == 0 {
                s.trim_start_matches('\u{feff}').into()
            } else {
                s.into()
            }
        })
        .collect();
    validate_headers(&headers)?;
    send(sender, Event::Header(headers))?;
    let mut batch = Vec::with_capacity(batch_rows);
    let mut count = 0_u64;
    for record in reader.records() {
        let record = record.context("Malformed CSV record")?;
        batch.push(
            record
                .iter()
                .map(|s| if s.is_empty() { None } else { Some(s.into()) })
                .collect(),
        );
        count += 1;
        if batch.len() == batch_rows {
            send(
                sender,
                Event::Rows(std::mem::replace(
                    &mut batch,
                    Vec::with_capacity(batch_rows),
                )),
            )?;
        }
    }
    if !batch.is_empty() {
        send(sender, Event::Rows(batch))?;
    }
    send(sender, Event::Done(count))
}

fn cell(range: &Range<Data>, row: usize, col: usize) -> Option<String> {
    match range.get_value((row as u32, col as u32)) {
        None | Some(Data::Empty) => None,
        Some(Data::String(s)) if s.is_empty() => None,
        Some(Data::DateTime(value)) => value
            .as_datetime()
            .map(|d| d.format("%Y-%m-%d %H:%M:%S").to_string()),
        Some(value) => Some(value.to_string()),
    }
}

fn meaningful(value: &Option<String>) -> bool {
    value.as_ref().is_some_and(|v| !v.trim().is_empty())
}

pub fn accounting_period(period: &str) -> Result<(String, i32)> {
    let (month, year) = period
        .trim()
        .split_once('-')
        .context("Accounting Period must be Mon-YY")?;
    let months = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let month = months
        .iter()
        .find(|m| m.eq_ignore_ascii_case(month))
        .context("Unknown accounting month")?;
    ensure!(
        year.len() == 2 && year.bytes().all(|b| b.is_ascii_digit()),
        "Accounting year must have two digits"
    );
    let year: i32 = year.parse()?;
    Ok((
        (*month).into(),
        if year <= 68 { 2000 + year } else { 1900 + year },
    ))
}

pub fn map_fund(value: &str) -> String {
    match value.trim() {
        "PSSAP Super" => "PSSAP",
        "PSS" => "PSS",
        "AustralianDefenceForceSuper" => "ADFS",
        "Parliamentary Cont Super Fund" => "PCS",
        "State Super Scheme Accounting" => "SAA",
        "SAS Trustee Corporation" => "STC",
        other => other,
    }
    .into()
}

fn parse_trial_balance(path: &Path, batch_rows: usize, sender: &mpsc::Sender<Event>) -> Result<()> {
    let mut workbook = open_workbook_auto(path).context("Reading Excel workbook")?;
    let sheet1 = workbook
        .worksheet_range("Sheet1")
        .context("Workbook needs Sheet1")?;
    let sheet2 = workbook
        .worksheet_range("Sheet2")
        .context("Workbook needs Sheet2")?;
    parse_trial_balance_ranges(&sheet1, &sheet2, batch_rows, sender)
}

fn parse_trial_balance_ranges(
    sheet1: &Range<Data>,
    sheet2: &Range<Data>,
    batch_rows: usize,
    sender: &mpsc::Sender<Event>,
) -> Result<()> {
    let parameters1 = [
        "Fund",
        "Report Date",
        "Ledger or Ledger Set",
        "Ledger Currency",
        "Currency Type",
        "Accounting Period",
        "Amount Type",
        "Balancing Segment",
        "Summarize By",
    ];
    let mut values1: BTreeMap<String, Option<String>> = BTreeMap::new();
    let mut report_row = None;
    for row in 0..14 {
        for col in 0..7 {
            if let Some(label) =
                cell(sheet1, row, col).filter(|s| parameters1.contains(&s.as_str()))
            {
                if label == "Report Date" {
                    report_row = Some(row);
                }
                values1.insert(label, cell(sheet1, row, col + 1));
            }
        }
    }
    if let Some(row) = report_row {
        values1.insert("Fund".into(), cell(sheet1, row + 1, 0));
    }
    let company = values1
        .get("Fund")
        .and_then(Option::as_deref)
        .context("Missing Sheet1 Fund")?;
    ensure!(!company.trim().is_empty(), "Sheet1 Fund is blank");
    let fund = map_fund(company);
    let (month, year) = accounting_period(
        values1
            .get("Accounting Period")
            .and_then(Option::as_deref)
            .context("Missing Accounting Period")?,
    )?;

    let parameters2 = ["Company", "Report Date", "Page", "Ledger", "FUND"];
    let mut values2: BTreeMap<String, Option<String>> = BTreeMap::new();
    let mut page_row = None;
    let mut account_header = None;
    for row in 0..20 {
        for col in 0..10 {
            if let Some(label) = cell(sheet2, row, col) {
                if label == "ACCOUNT" {
                    account_header = Some((row, col));
                }
                if parameters2.contains(&label.as_str()) {
                    values2.insert(
                        label.clone(),
                        (col + 1..10)
                            .map(|c| cell(sheet2, row, c))
                            .find(meaningful)
                            .flatten(),
                    );
                    if label == "Page" {
                        page_row = Some(row);
                    }
                }
            }
        }
    }
    let company2 = page_row.and_then(|row| cell(sheet2, row, 0));
    let (header, account_col) = account_header.context("No ACCOUNT header in Sheet2 rows 1..20")?;
    // The source looked only within the first 20 rows. Search the complete sheet here.
    let end = sheet2.end().context("Sheet2 is empty")?.0 as usize;
    let start = (header + 1..=end)
        .find(|r| meaningful(&cell(sheet2, *r, account_col)))
        .context("No trial-balance data after ACCOUNT")?;
    let columns: Vec<String> = [
        "account",
        "description",
        "account_long",
        "Begining_balance_AUD",
        "Debits_AUD",
        "Credits_AUD",
        "Ending_Balance_AUD",
        "Fund",
        "Year",
        "Month",
        "x_row_num",
    ]
    .into_iter()
    .map(String::from)
    .collect();
    send(sender, Event::Header(columns))?;
    let mut count = 0_u64;
    let mut batch = Vec::with_capacity(batch_rows);
    let mut totals: Option<Row> = None;
    for row in start..=end {
        let all: Row = (0..11).map(|c| cell(sheet2, row, c)).collect();
        if !all.iter().any(meaningful) {
            continue;
        }
        if let Some(total_index) = all
            .iter()
            .position(|v| v.as_ref().is_some_and(|s| s.starts_with("Total")))
        {
            let numeric: Row = all[total_index + 1..]
                .iter()
                .filter(|v| meaningful(v))
                .cloned()
                .collect();
            if numeric.len() >= 4 {
                totals = Some(numeric[..4].to_vec());
            }
        }
        let mut output: Row = [0, 1, 3, 5, 6, 8, 10]
            .into_iter()
            .map(|c| all[c].clone())
            .collect();
        output.extend([
            Some(fund.clone()),
            Some(year.to_string()),
            Some(month.clone()),
            Some(count.to_string()),
        ]);
        batch.push(output);
        count += 1;
        if batch.len() == batch_rows {
            send(
                sender,
                Event::Rows(std::mem::replace(
                    &mut batch,
                    Vec::with_capacity(batch_rows),
                )),
            )?;
        }
    }
    if !batch.is_empty() {
        send(sender, Event::Rows(batch))?;
    }
    let totals = totals.context("No Total row with four balances found in Sheet2")?;
    let mut metadata: Vec<(String, Option<String>)> = vec![
        ("x_fund".into(), Some(fund)),
        ("x_year".into(), Some(year.to_string())),
        ("x_month".into(), Some(month)),
    ];
    for (column, key) in [
        ("company_name_sheet1", "Fund"),
        ("report_date_sheet1", "Report Date"),
        ("ledger_or_ledger_set_sheet1", "Ledger or Ledger Set"),
        ("currency_type", "Currency Type"),
        ("ledger_currency", "Ledger Currency"),
        ("accounting_period", "Accounting Period"),
        ("amount_type", "Amount Type"),
        ("summarize_by", "Summarize By"),
    ] {
        metadata.push((column.into(), values1.get(key).cloned().flatten()));
    }
    if let Some(value) = values1.get("Balancing Segment").filter(|v| meaningful(v)) {
        metadata.push(("balancing_segment".into(), value.clone()));
    }
    metadata.extend([
        (
            "report_date_sheet2".into(),
            values2.get("Report Date").cloned().flatten(),
        ),
        ("company_name_sheet2".into(), company2),
        (
            "ledger_sheet2".into(),
            values2.get("Ledger").cloned().flatten(),
        ),
        ("page_sheet2".into(), values2.get("Page").cloned().flatten()),
        ("fund_sheet2".into(), values2.get("FUND").cloned().flatten()),
    ]);
    for (key, value) in [
        "beginning_balance_aud_total",
        "debits_aud_total",
        "credits_aud_total",
        "ending_balance_aud_total",
    ]
    .into_iter()
    .zip(totals)
    {
        metadata.push((key.into(), value));
    }
    let (columns, row) = metadata.into_iter().unzip();
    send(sender, Event::Metadata { columns, row })?;
    send(sender, Event::Done(count))
}

#[derive(Serialize)]
pub struct Inspection {
    pub columns: Vec<String>,
    pub rows: u64,
    pub sample: Vec<Row>,
    pub metadata: BTreeMap<String, Option<String>>,
}

pub async fn inspect(path: &Path, kind: Kind) -> Result<Inspection> {
    // Local inspection uses the same parser without Azure or SQL credentials.
    let path = path.to_owned();
    let (sender, mut events) = mpsc::channel(2);
    let task = tokio::task::spawn_blocking(move || match kind {
        Kind::Csv => parse_csv(std::fs::File::open(&path)?, 1000, &sender),
        Kind::TrialBalance => parse_trial_balance(&path, 1000, &sender),
    });
    let mut output = Inspection {
        columns: Vec::new(),
        rows: 0,
        sample: Vec::new(),
        metadata: BTreeMap::new(),
    };
    let mut done = false;
    while let Some(event) = events.recv().await {
        match event {
            Event::Header(columns) => output.columns = columns,
            Event::Rows(rows) => {
                let remaining = 3_usize.saturating_sub(output.sample.len());
                output.sample.extend(rows.into_iter().take(remaining));
            }
            Event::Metadata { columns, row } => {
                output.metadata = columns.into_iter().zip(row).collect()
            }
            Event::Done(rows) => {
                output.rows = rows;
                done = true;
            }
        }
    }
    task.await.context("Parser task panicked")??;
    if !done {
        bail!("Parser did not report completion");
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn csv_multiline_nulls_and_exact_counts() {
        let mut file = NamedTempFile::new().unwrap();
        use std::io::Write;
        file.write_all(
            b"id,description,value\r\n1,\"line one\nline two\",\r\n2,\"a \"\"quote\"\"\",0\r\n",
        )
        .unwrap();
        let result = inspect(file.path(), Kind::Csv).await.unwrap();
        assert_eq!(result.rows, 2);
        assert_eq!(result.sample[0][1].as_deref(), Some("line one\nline two"));
        assert_eq!(result.sample[0][2], None);
        assert_eq!(result.sample[1][1].as_deref(), Some("a \"quote\""));
        assert_eq!(result.sample[1][2].as_deref(), Some("0"));
    }
    #[tokio::test]
    async fn csv_rejects_inconsistent_row_width() {
        let mut file = NamedTempFile::new().unwrap();
        use std::io::Write;
        file.write_all(b"id,name\n1,ok\n2,bad,extra\n").unwrap();
        assert!(inspect(file.path(), Kind::Csv).await.is_err());
    }
    #[test]
    fn accounting_period_preserves_python_year_pivot() {
        assert_eq!(accounting_period("Apr-26").unwrap(), ("Apr".into(), 2026));
        assert_eq!(accounting_period("Jan-68").unwrap().1, 2068);
        assert_eq!(accounting_period("Jan-69").unwrap().1, 1969);
        assert!(accounting_period("13-26").is_err());
        assert_eq!(map_fund("SAS Trustee Corporation"), "STC");
    }
    #[test]
    fn csv_headers_cannot_collide_with_audit_columns() {
        assert!(validate_headers(&["id".into(), "ID".into()]).is_err());
        assert!(validate_headers(&["x_run_id".into()]).is_err());
    }
}
