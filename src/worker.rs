use crate::{
    config::Family,
    db::{Database, RunContext},
    parser::{self, Event, Kind, Row},
    storage::{InputFile, Storage, relative_file},
};
use anyhow::{Context, Result, ensure};
use chrono::Utc;
use serde::Serialize;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerResult {
    pub status: String,
    pub wave_id: usize,
    pub worker_id: usize,
    pub files_received: usize,
    pub files_loaded: usize,
    pub files_failed: usize,
    pub rows_loaded: u64,
    pub errors: Vec<FileError>,
}
#[derive(Debug, Serialize)]
pub struct FileError {
    pub filename: String,
    pub error_message: String,
}

fn raw_context(
    db: &Database,
    ctx: &RunContext,
    relative: &str,
    timestamp: &str,
) -> (Vec<String>, Row) {
    // SQL names follow the source notebooks; metadata uses the same spelling.
    (
        ["x_drop", "x_SourceFile", "x_Run_Id", "x_LoadTimestamp"]
            .into_iter()
            .map(String::from)
            .collect(),
        vec![
            Some(db.settings.config.drop_id.to_string()),
            Some(relative.into()),
            Some(ctx.run_id.to_string()),
            Some(timestamp.into()),
        ],
    )
}

async fn attempt(
    db: &Database,
    storage: &Storage,
    ctx: &RunContext,
    file: &InputFile,
    kind: Kind,
) -> Result<u64> {
    let relative = relative_file(&file.path, &db.settings.input())?;
    let mut connection = db.connect().await?;
    if let Some(previous) = db
        .file_record(&mut connection, ctx, &relative, false)
        .await?
        .filter(|record| record.status == "SUCCESS")
    {
        let checksum = previous
            .checksum
            .context("Successful audit record has no checksum")?;
        let destination = format!("{}/{}/{}", db.settings.folder("Out"), ctx.run_id, relative);
        storage.move_loaded(file, &destination, &checksum).await?;
        return u64::try_from(previous.rows).context("Negative audit row count");
    }
    let download = storage.download(file).await?;
    let previous = db
        .begin_file(&mut connection, ctx, &relative, &file.name, file.size)
        .await?;
    let count = if let Some(previous) = previous {
        ensure!(
            previous.checksum.as_deref() == Some(download.checksum.as_str()),
            "This run already loaded different content at the same file path"
        );
        u64::try_from(previous.rows).context("Negative audit row count")?
    } else {
        db.checksum(&mut connection, ctx, &relative, &download.checksum)
            .await
            .context("Audit checksum update failed")?;
        let stream = parser::spawn(
            download.file,
            kind,
            db.settings.config.batch_rows,
            db.settings.config.parser_queue_batches,
        );
        let mut events = stream.events;
        let mut header: Option<Vec<String>> = None;
        let mut inserted = 0_u64;
        let mut done = None;
        let mut metadata_written = false;
        let timestamp = Utc::now()
            .naive_utc()
            .format("%Y-%m-%dT%H:%M:%S%.6f")
            .to_string();
        let (extra_columns, extra_values) = raw_context(db, ctx, &relative, &timestamp);
        while let Some(event) = events.recv().await {
            match event {
                Event::Header(mut columns) => {
                    ensure!(header.is_none(), "Parser sent multiple headers");
                    columns.extend(extra_columns.clone());
                    header = Some(columns);
                }
                Event::Rows(mut rows) => {
                    let columns = header.as_ref().context("Parser sent data before headers")?;
                    for row in &mut rows {
                        row.extend(extra_values.clone());
                    }
                    connection
                        .insert(&db.settings.target_table, columns, &rows)
                        .await?;
                    inserted += rows.len() as u64;
                }
                Event::Metadata {
                    mut columns,
                    mut row,
                } => {
                    ensure!(!metadata_written, "Parser sent multiple metadata records");
                    columns.extend(
                        ["x_drop", "x_run_id", "environment", "x_sourcefile"]
                            .into_iter()
                            .map(String::from),
                    );
                    row.extend([
                        Some(db.settings.config.drop_id.to_string()),
                        Some(ctx.run_id.to_string()),
                        Some(db.settings.config.environment.clone()),
                        Some(relative.clone()),
                    ]);
                    connection
                        .insert(
                            &format!("{}.gl_tb_metadata", db.settings.schema),
                            &columns,
                            &[row],
                        )
                        .await?;
                    metadata_written = true;
                }
                Event::Done(count) => {
                    ensure!(done.is_none(), "Multiple completion events");
                    done = Some(count);
                }
            }
        }
        stream.task.await.context("Parser task panicked")??;
        let count = done.context("Parser did not report completion")?;
        ensure!(inserted == count, "Parser and insert row counts differ");
        ensure!(
            !matches!(kind, Kind::TrialBalance) || metadata_written,
            "Trial balance has no metadata"
        );
        db.complete_file(&mut connection, ctx, &relative, count)
            .await?;
        count
    };
    drop(connection);
    // Moving comes after commit. A failed move leaves SUCCESS in audit and can be retried safely.
    let destination = format!("{}/{}/{}", db.settings.folder("Out"), ctx.run_id, relative);
    storage
        .move_file(
            &file.path,
            &destination,
            &download.checksum,
            Some(&download.etag),
        )
        .await?;
    Ok(count)
}

pub async fn run(
    db: Database,
    storage: Storage,
    ctx: RunContext,
    files: Vec<InputFile>,
    wave_id: usize,
    worker_id: usize,
    cancel: CancellationToken,
) -> Result<WorkerResult> {
    let kind = if db.settings.family == Family::Tb {
        Kind::TrialBalance
    } else {
        Kind::Csv
    };
    ensure!(
        db.settings.family != Family::Checkpoint,
        "Checkpoint cannot run as a file worker"
    );
    let mut result = WorkerResult {
        status: "SUCCESS".into(),
        wave_id,
        worker_id,
        files_received: files.len(),
        files_loaded: 0,
        files_failed: 0,
        rows_loaded: 0,
        errors: Vec::new(),
    };
    for file in files {
        if cancel.is_cancelled() {
            anyhow::bail!("Worker cancelled");
        }
        let relative = relative_file(&file.path, &db.settings.input())?;
        let mut failure = None;
        let mut loaded = false;
        for retry in 0..=db.settings.config.worker_retries {
            if retry > 0 {
                tokio::select! {
                    _ = cancel.cancelled() => anyhow::bail!("Worker cancelled"),
                    _ = tokio::time::sleep(Duration::from_secs(db.settings.config.retry_interval_seconds)) => {}
                }
            }
            let attempt = tokio::time::timeout(
                Duration::from_secs(db.settings.config.file_timeout_seconds),
                attempt(&db, &storage, &ctx, &file, kind),
            );
            let outcome = tokio::select! {
                _ = cancel.cancelled() => anyhow::bail!("Worker cancelled"),
                outcome = attempt => outcome.context("File load timed out").and_then(|r| r),
            };
            match outcome {
                Ok(rows) => {
                    result.files_loaded += 1;
                    result.rows_loaded += rows;
                    loaded = true;
                    break;
                }
                Err(error) => {
                    tracing::warn!(wave_id, worker_id, retry, file=%relative, error=%error, "File attempt failed");
                    failure = Some(format!("{error:#}"));
                }
            }
        }
        if loaded {
            continue;
        }
        let error = failure.context("No file attempt result")?;
        let mut connection = db.connect().await?;
        let failed = db
            .fail_file(
                &mut connection,
                &ctx,
                &relative,
                &file.name,
                file.size,
                &error,
            )
            .await?;
        if failed {
            // Snapshot checks prevent archiving a replacement uploaded at the same path.
            let moved = async {
                let download = storage.download(&file).await?;
                let destination = format!(
                    "{}/{}/{}",
                    db.settings.folder("Error"),
                    ctx.run_id,
                    relative
                );
                storage
                    .move_file(
                        &file.path,
                        &destination,
                        &download.checksum,
                        Some(&download.etag),
                    )
                    .await
            }
            .await;
            if let Err(move_error) = moved {
                db.message(
                    &mut connection,
                    ctx.run_id,
                    &format!("Failed file {relative} remains in In: {move_error:#}"),
                )
                .await?;
            }
        } else {
            db.message(
                &mut connection,
                ctx.run_id,
                &format!("Committed file {relative} needs its Out move repaired: {error}"),
            )
            .await?;
        }
        result.files_failed += 1;
        result.errors.push(FileError {
            filename: relative,
            error_message: error,
        });
    }
    if result.files_failed > 0 {
        result.status = "FAILED".into();
    }
    Ok(result)
}
