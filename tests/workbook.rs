use fa_load::parser::{self, Kind};
use std::io::Write;
use tempfile::NamedTempFile;
use zip::{ZipWriter, write::SimpleFileOptions};

fn workbook(page_label: bool, first_data_row: usize) -> NamedTempFile {
    let file = tempfile::Builder::new().suffix(".xlsx").tempfile().unwrap();
    let writer = std::fs::File::create(file.path()).unwrap();
    let mut zip = ZipWriter::new(writer);
    let content_types = r#"<?xml version="1.0"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/><Override PartName="/xl/worksheets/sheet2.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/></Types>"#;
    let root_rels = r#"<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>"#;
    let workbook = r#"<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Sheet1" sheetId="1" r:id="rId1"/><sheet name="Sheet2" sheetId="2" r:id="rId2"/></sheets></workbook>"#;
    let workbook_rels = r#"<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/><Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet2.xml"/></Relationships>"#;
    let sheet1 = r#"<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData><row r="1"><c r="A1" t="inlineStr"><is><t>Report Date</t></is></c><c r="B1" t="inlineStr"><is><t>2026-04-30</t></is></c></row><row r="2"><c r="A2" t="inlineStr"><is><t>PSSAP Super</t></is></c></row><row r="3"><c r="A3" t="inlineStr"><is><t>Accounting Period</t></is></c><c r="B3" t="inlineStr"><is><t>Apr-26</t></is></c></row></sheetData></worksheet>"#;
    let page = if page_label {
        r#"<c r="F1" t="inlineStr"><is><t>Page</t></is></c><c r="H1"><v>1</v></c>"#
    } else {
        ""
    };
    let total_row = first_data_row + 1;
    let end_row = first_data_row + 2;
    let sheet2 = format!(
        r#"<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>
<row r="1"><c r="A1" t="inlineStr"><is><t>PSSAP Super</t></is></c>{page}</row>
<row r="12"><c r="A12" t="inlineStr"><is><t>ACCOUNT</t></is></c></row>
<row r="{first_data_row}"><c r="A{first_data_row}" t="inlineStr"><is><t>100100</t></is></c><c r="B{first_data_row}" t="inlineStr"><is><t>Cash</t></is></c><c r="D{first_data_row}" t="inlineStr"><is><t>06.100100</t></is></c><c r="F{first_data_row}"><v>100</v></c><c r="G{first_data_row}"><v>10</v></c><c r="I{first_data_row}"><v>5</v></c><c r="K{first_data_row}"><v>105</v></c></row>
<row r="{total_row}"><c r="A{total_row}" t="inlineStr"><is><t>Total for FUND PSSAP</t></is></c><c r="F{total_row}"><v>100</v></c><c r="G{total_row}"><v>10</v></c><c r="I{total_row}"><v>5</v></c><c r="K{total_row}"><v>105</v></c></row>
<row r="{end_row}"><c r="A{end_row}" t="inlineStr"><is><t>End of Report</t></is></c></row>
</sheetData></worksheet>"#
    );
    for (name, text) in [
        ("[Content_Types].xml", content_types),
        ("_rels/.rels", root_rels),
        ("xl/workbook.xml", workbook),
        ("xl/_rels/workbook.xml.rels", workbook_rels),
        ("xl/worksheets/sheet1.xml", sheet1),
        ("xl/worksheets/sheet2.xml", &sheet2),
    ] {
        zip.start_file(name, SimpleFileOptions::default()).unwrap();
        zip.write_all(text.as_bytes()).unwrap();
    }
    zip.finish().unwrap();
    file
}

#[tokio::test]
async fn real_xlsx_metadata_columns_footer_and_counts() {
    let file = workbook(true, 13);
    let output = parser::inspect(file.path(), Kind::TrialBalance)
        .await
        .unwrap();
    assert_eq!(output.rows, 3); // Preserve source footer rows in the raw table.
    assert_eq!(output.sample[0][2].as_deref(), Some("06.100100"));
    assert_eq!(output.sample[0][6].as_deref(), Some("105"));
    assert_eq!(output.sample[0][7].as_deref(), Some("PSSAP"));
    assert_eq!(output.metadata["x_year"].as_deref(), Some("2026"));
    assert_eq!(output.metadata["credits_aud_total"].as_deref(), Some("5"));
    assert_eq!(output.metadata["page_sheet2"].as_deref(), Some("1"));
}

#[tokio::test]
async fn missing_page_label_and_data_beyond_twenty_rows_do_not_panic() {
    let file = workbook(false, 23);
    let output = parser::inspect(file.path(), Kind::TrialBalance)
        .await
        .unwrap();
    assert_eq!(output.rows, 3);
    assert_eq!(output.metadata["page_sheet2"], None);
}

#[tokio::test]
async fn blocking_csv_parser_stops_when_bounded_consumer_drops() {
    let mut file = NamedTempFile::new().unwrap();
    file.write_all(b"id\n1\n2\n3\n4\n5\n").unwrap();
    let stream = parser::spawn(file, Kind::Csv, 1, 1);
    drop(stream.events);
    let result = tokio::time::timeout(std::time::Duration::from_secs(2), stream.task)
        .await
        .unwrap()
        .unwrap();
    assert!(result.is_err());
}
