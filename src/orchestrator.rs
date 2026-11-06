use crate::{
    config::Family,
    db::{Completion, Database, Db, RunContext, Stats},
    storage::{InputFile, Storage, relative_file},
    worker,
};
use anyhow::{Context, Result, bail, ensure};
use serde::Serialize;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Summary {
    pub status: String,
    pub run_id: Option<i64>,
    pub operation: String,
    pub files_in_run: i64,
    pub counts: Stats,
    pub db_rows_loaded: i64,
    pub waves_executed: usize,
    pub message: String,
}

async fn wave(
    db: &Database,
    storage: &Storage,
    ctx: &RunContext,
    files: &[InputFile],
    wave_id: usize,
    cancel: &CancellationToken,
) -> Result<Vec<worker::WorkerResult>> {
    let mut tasks = JoinSet::new();
    for (index, chunk) in files
        .chunks(db.settings.config.files_per_worker)
        .enumerate()
    {
        tasks.spawn(worker::run(
            db.clone(),
            storage.clone(),
            ctx.clone(),
            chunk.to_vec(),
            wave_id,
            index + 1,
            cancel.child_token(),
        ));
    }
    let deadline = tokio::time::sleep(Duration::from_secs(db.settings.config.wave_timeout_seconds));
    tokio::pin!(deadline);
    let mut results = Vec::new();
    let mut error = None;
    loop {
        let next = tokio::select! {
            _ = cancel.cancelled() => { error = Some(anyhow::anyhow!("Run cancelled")); break; }
            _ = &mut deadline => { error = Some(anyhow::anyhow!("Wave timed out")); break; }
            next = tasks.join_next() => next,
        };
        match next {
            None => break,
            Some(Ok(Ok(result))) => results.push(result),
            Some(Ok(Err(worker_error))) => {
                error = Some(worker_error);
                break;
            }
            Some(Err(join_error)) => {
                error = Some(join_error.into());
                break;
            }
        }
    }
    if let Some(error) = error {
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        return Err(error);
    }
    Ok(results)
}

async fn promote(db: &Database, storage: &Storage, ctx: &RunContext) -> Result<()> {
    let out = format!("{}/{}", db.settings.folder("Out"), ctx.run_id);
    let pass = format!("{}/{}", db.settings.folder("Pass"), ctx.run_id);
    // Promote files individually, so a partially completed promotion is restartable.
    for file in storage.list(&out).await? {
        let relative = relative_file(&file.path, &out)?;
        let mut connection = db.connect().await?;
        let record = db
            .file_record(&mut connection, ctx, &relative, false)
            .await?
            .context("Out file has no audit record")?;
        ensure!(
            record.status == "SUCCESS",
            "Out file is not successfully loaded: {relative}"
        );
        let checksum = record.checksum.context("Out file has no checksum")?;
        // Verify Out bytes too: they may have changed since the original input rename.
        let download = storage.download(&file).await?;
        ensure!(
            download.checksum == checksum,
            "Out file changed after loading: {relative}"
        );
        storage
            .move_file(
                &file.path,
                &format!("{pass}/{relative}"),
                &checksum,
                Some(&download.etag),
            )
            .await?;
    }
    let names = db.audit_names(&mut db.connect().await?, ctx).await?;
    let archived = storage.list(&pass).await?;
    ensure!(
        archived.len() == names.len(),
        "Pass file count differs from audited files"
    );
    for file in archived {
        ensure!(
            names.contains(&relative_file(&file.path, &pass)?),
            "Pass contains an unaudited file"
        );
    }
    Ok(())
}

struct LoadPlan<'a> {
    skip_pre: bool,
    files: &'a [InputFile],
    total: i64,
}

async fn load(
    db: &Database,
    storage: &Storage,
    connection: &mut Db,
    ctx: &RunContext,
    plan: LoadPlan<'_>,
    cancel: &CancellationToken,
    waves: &mut usize,
) -> Result<&'static str> {
    let LoadPlan {
        skip_pre,
        files,
        total,
    } = plan;
    let initial_stats = db.stats(connection, ctx).await?;
    if files.is_empty() && initial_stats.files_loaded == 0 {
        db.check_status(
            connection,
            ctx,
            "pre_check",
            "FAIL",
            "No matching files in input folder",
        )
        .await?;
        return Ok("FAILED");
    }
    if !skip_pre {
        if let Some(expected) = db.expected(connection, "Pre", false).await? {
            if expected != total {
                db.check_status(
                    connection,
                    ctx,
                    "pre_check",
                    "FAIL",
                    &format!("Expected {expected} files; found {total}"),
                )
                .await?;
                return Ok("BLOCKED");
            }
        }
        if !db.sql_check(connection, ctx, "PRE_CHECK").await? {
            return Ok("BLOCKED");
        }
    }
    let wave_size = db.settings.concurrency * db.settings.config.files_per_worker;
    let mut worker_errors = false;
    for files in files.chunks(wave_size) {
        *waves += 1;
        let results = wave(db, storage, ctx, files, *waves, cancel).await?;
        worker_errors |= results.iter().any(|r| r.files_failed > 0);
        tracing::info!(wave=*waves, results=?results, "Wave finished");
        let stats = db.stats(connection, ctx).await?;
        db.progress(connection, ctx, total, &stats).await?;
    }
    let stats = db.stats(connection, ctx).await?;
    if worker_errors
        || stats.files_failed > 0
        || stats.files_pending > 0
        || stats.files_loaded != total
    {
        db.check_status(
            connection,
            ctx,
            "post_check",
            "FAIL",
            "One or more files failed, remain pending, or need an archive move repaired",
        )
        .await?;
        return Ok("FAILED");
    }
    let actual = db
        .row_count(connection, &db.settings.target_table, ctx.run_id)
        .await?;
    let expected = db.expected(connection, "Post", true).await?;
    let metadata = if db.settings.family == Family::Tb {
        Some(
            db.row_count(
                connection,
                &format!("{}.gl_tb_metadata", db.settings.schema),
                ctx.run_id,
            )
            .await?,
        )
    } else {
        None
    };
    if let Err(error) = reconcile(&stats, total, actual, expected, metadata) {
        db.check_status(connection, ctx, "post_check", "FAIL", &error.to_string())
            .await?;
        return Ok("FAILED");
    }
    if !db.sql_check(connection, ctx, "POST_CHECK").await? {
        return Ok("FAILED");
    }
    promote(db, storage, ctx).await?;
    Ok("SUCCESS")
}

pub async fn run(
    db: Database,
    storage: Storage,
    resume_started: bool,
    cancel: CancellationToken,
) -> Result<Summary> {
    let mut connection = db.connect().await?;
    if db.settings.family == Family::Checkpoint {
        let passed = db.checkpoint(&mut connection).await?;
        let status = if passed { "SUCCESS" } else { "FAILED" };
        db.message(&mut connection, 0, &format!("Checkpoint: {status}"))
            .await?;
        return Ok(Summary {
            status: status.into(),
            run_id: None,
            operation: "Pipeline_Checkpoint".into(),
            files_in_run: 0,
            counts: Stats::default(),
            db_rows_loaded: 0,
            waves_executed: 0,
            message: "Latest run of each of six load families must succeed in this environment"
                .into(),
        });
    }
    db.lock_run(&mut connection).await?;
    let previous = db.previous_run(&mut connection).await?;
    if let Some(previous) = &previous {
        ensure!(
            previous.status != "STARTED" || resume_started,
            "Latest run {} is STARTED; after checking the old process is stopped, use --resume-started",
            previous.run_id
        );
    }
    let (ctx, skip_pre) = match previous {
        None => (db.start_run(&mut connection, true).await?, false),
        Some(previous) if previous.status == "SUCCESS" => {
            (db.start_run(&mut connection, false).await?, false)
        }
        Some(previous) => {
            let ctx = RunContext {
                run_id: previous.run_id,
                operation: previous.operation,
            };
            db.restart_run(&mut connection, &ctx).await?;
            (ctx, previous.pre_check.as_deref() == Some("PASS"))
        }
    };
    let mut waves = 0;
    let mut total = 0_i64;
    // Errors after run creation still pass through finalisation and get a non-success exit.
    let outcome: Result<&str> = async {
        let mut files = storage.list(&db.settings.input()).await?;
        files.retain(|f| db.settings.family.matches_path(&f.path));
        let mut names = db.audit_names(&mut connection, &ctx).await?;
        for file in &files {
            names.insert(relative_file(&file.path, &db.settings.input())?);
        }
        total = i64::try_from(names.len())?;
        let stats = db.stats(&mut connection, &ctx).await?;
        db.progress(&mut connection, &ctx, total, &stats).await?;
        load(
            &db,
            &storage,
            &mut connection,
            &ctx,
            LoadPlan {
                skip_pre,
                files: &files,
                total,
            },
            &cancel,
            &mut waves,
        )
        .await
    }
    .await;
    let (status, message) = match outcome {
        Ok(status) => (
            status,
            if status == "SUCCESS" {
                "Load and post-checks passed".into()
            } else {
                "Review audit$file, run$message, and run$sql_log".into()
            },
        ),
        Err(error) => {
            let message = format!("{error:#}");
            db.check_status(&mut connection, &ctx, "post_check", "ERROR", &message)
                .await?;
            ("ERRORED", message)
        }
    };
    let counts = db.stats(&mut connection, &ctx).await?;
    let db_rows = db
        .row_count(&mut connection, &db.settings.target_table, ctx.run_id)
        .await?;
    db.finish(
        &mut connection,
        &ctx,
        Completion {
            status,
            total,
            stats: &counts,
            db_rows,
            message: &message,
        },
    )
    .await?;
    if let Err(error) = storage.remove_empty_directories(&db.settings.input()).await {
        tracing::warn!(error=%error, "Empty input directory cleanup failed");
        db.message(
            &mut connection,
            ctx.run_id,
            &format!("Input directory cleanup: {error:#}"),
        )
        .await?;
    }
    Ok(Summary {
        status: status.into(),
        run_id: Some(ctx.run_id),
        operation: ctx.operation,
        files_in_run: total,
        counts,
        db_rows_loaded: db_rows,
        waves_executed: waves,
        message,
    })
}

pub fn reconcile(
    stats: &Stats,
    total: i64,
    db_rows: i64,
    expected_rows: Option<i64>,
    metadata_rows: Option<i64>,
) -> Result<()> {
    ensure!(
        stats.files_failed == 0 && stats.files_pending == 0 && stats.files_loaded == total,
        "Post-check: loaded files do not match the complete run"
    );
    ensure!(
        db_rows == stats.rows_loaded,
        "Post-check: database has {db_rows} rows; audit has {}",
        stats.rows_loaded
    );
    if let Some(expected) = expected_rows {
        ensure!(
            db_rows == expected,
            "Post-check: expected {expected} rows; database has {db_rows}"
        );
    }
    if let Some(metadata) = metadata_rows {
        ensure!(
            metadata == stats.files_loaded,
            "Post-check: metadata rows {metadata} differ from loaded files {}",
            stats.files_loaded
        );
    }
    Ok(())
}

pub async fn manual_worker(
    db: Database,
    storage: Storage,
    ctx: RunContext,
    files: Vec<InputFile>,
    wave_id: usize,
    worker_id: usize,
    cancel: CancellationToken,
) -> Result<worker::WorkerResult> {
    let RunContext { run_id, operation } = ctx;
    ensure!(
        Family::parse(&operation)? == db.settings.family,
        "Worker operation differs from configured family"
    );
    ensure!(
        operation.starts_with("Initial_") || operation.starts_with("Incremental_"),
        "Worker needs a full audit operation such as Initial_GL_Load"
    );
    let files = files
        .into_iter()
        .map(|f| storage.normalise_file(f))
        .collect::<Result<Vec<_>>>()?;
    let mut connection = db.connect().await?;
    let rows = connection.query(&format!("SELECT status FROM {} WHERE run_id=@P1 AND environment=@P2 AND operation=@P3 AND target_table=@P4", db.table("run$log")?),
        &[run_id.into(), db.settings.config.environment.as_str().into(), operation.as_str().into(), db.settings.target_table.as_str().into()]).await?;
    let Some(row) = rows.first() else {
        bail!("Manual worker requires an existing matching run$log row");
    };
    ensure!(
        row.try_get::<&str, _>(0)? == Some("STARTED"),
        "Manual worker requires a STARTED run"
    );
    worker::run(
        db,
        storage,
        RunContext { run_id, operation },
        files,
        wave_id,
        worker_id,
        cancel,
    )
    .await
}
