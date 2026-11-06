use crate::{
    auth::{SQL_RESOURCE, Tokens},
    config::{Resolved, quote_identifier, quote_table},
    parser::Row as DataRow,
};
use anyhow::{Context, Result, bail, ensure};
use serde::Serialize;
use std::{collections::HashSet, sync::Arc, time::Duration};
use tiberius::{AuthMethod, Client, Config as TdsConfig, EncryptionLevel, Query, Row};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

pub const NOW: &str =
    "SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'AUS Eastern Standard Time'";
type TdsClient = Client<Compat<TcpStream>>;

#[derive(Clone)]
pub enum Value {
    Text(Option<String>),
    Int(i64),
}
impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Self::Text(Some(v.into()))
    }
}
impl From<String> for Value {
    fn from(v: String) -> Self {
        Self::Text(Some(v))
    }
}
impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Self::Int(v)
    }
}

pub struct Db {
    client: TdsClient,
}
#[derive(Clone)]
pub struct Database {
    pub settings: Arc<Resolved>,
    tokens: Arc<Tokens>,
}
#[derive(Clone, Debug, Serialize)]
pub struct RunContext {
    pub run_id: i64,
    pub operation: String,
}
#[derive(Debug)]
pub struct PreviousRun {
    pub run_id: i64,
    pub operation: String,
    pub status: String,
    pub pre_check: Option<String>,
}
#[derive(Clone, Debug)]
pub struct FileRecord {
    pub status: String,
    pub rows: i64,
    pub checksum: Option<String>,
}
#[derive(Default, Clone, Debug, Serialize)]
pub struct Stats {
    pub files_loaded: i64,
    pub files_failed: i64,
    pub files_pending: i64,
    pub rows_loaded: i64,
}

pub struct Completion<'a> {
    pub status: &'a str,
    pub total: i64,
    pub stats: &'a Stats,
    pub db_rows: i64,
    pub message: &'a str,
}

fn number(row: &Row, index: usize) -> Result<i64> {
    if let Ok(Some(value)) = row.try_get::<i64, _>(index) {
        return Ok(value);
    }
    if let Ok(Some(value)) = row.try_get::<i32, _>(index) {
        return Ok(value as i64);
    }
    if let Ok(Some(value)) = row.try_get::<i16, _>(index) {
        return Ok(value as i64);
    }
    if let Ok(Some(value)) = row.try_get::<u8, _>(index) {
        return Ok(value as i64);
    }
    if let Ok(Some(value)) = row.try_get::<bool, _>(index) {
        return Ok(i64::from(value));
    }
    bail!("Expected SQL integer in column {index}")
}
fn text(row: &Row, index: usize) -> Result<String> {
    Ok(row
        .try_get::<&str, _>(index)?
        .context("Unexpected SQL NULL")?
        .into())
}

impl Database {
    pub fn new(settings: Arc<Resolved>, tokens: Arc<Tokens>) -> Self {
        Self { settings, tokens }
    }
    pub async fn connect(&self) -> Result<Db> {
        let config = &self.settings.config.sql;
        let token = self.tokens.get(SQL_RESOURCE).await?;
        let mut tds = TdsConfig::new();
        tds.host(&config.server);
        tds.port(config.port);
        tds.database(&config.database);
        tds.authentication(AuthMethod::aad_token(token.as_ref()));
        tds.encryption(EncryptionLevel::Required);
        // Certificate validation stays enabled; do not call trust_cert().
        let client =
            tokio::time::timeout(Duration::from_secs(config.connect_timeout_seconds), async {
                let tcp = TcpStream::connect(tds.get_addr()).await?;
                tcp.set_nodelay(true)?;
                Client::connect(tds, tcp.compat_write())
                    .await
                    .map_err(anyhow::Error::from)
            })
            .await
            .context("SQL connection timed out")??;
        Ok(Db { client })
    }
    pub fn table(&self, suffix: &str) -> Result<String> {
        quote_table(&format!("{}.{suffix}", self.settings.schema))
    }
    pub fn key(&self, ctx: &RunContext, relative: &str) -> Vec<Value> {
        vec![
            ctx.run_id.into(),
            self.settings.config.environment.as_str().into(),
            ctx.operation.as_str().into(),
            relative.into(),
        ]
    }

    pub async fn lock_run(&self, db: &mut Db) -> Result<()> {
        let key = format!(
            "fa-load:{}:{}:{}",
            self.settings.config.environment,
            self.settings.family.data(),
            self.settings.target_table
        );
        let rows = db.query("DECLARE @result int; EXEC @result = sys.sp_getapplock @Resource=@P1, @LockMode='Exclusive', @LockOwner='Session', @LockTimeout=0; SELECT @result;", &[key.into()]).await?;
        ensure!(
            number(rows.first().context("No lock result")?, 0)? >= 0,
            "This operation is already running in another Rust loader"
        );
        Ok(())
    }

    pub async fn previous_run(&self, db: &mut Db) -> Result<Option<PreviousRun>> {
        let rows = db.query(&format!("SELECT TOP 1 CAST(run_id AS bigint), operation, status, pre_check FROM {} WHERE operation_data=@P1 AND environment=@P2 AND target_table=@P3 ORDER BY run_id DESC", self.table("run$log")?),
            &[self.settings.family.data().into(), self.settings.config.environment.as_str().into(), self.settings.target_table.as_str().into()]).await?;
        rows.first()
            .map(|row| {
                Ok(PreviousRun {
                    run_id: number(row, 0)?,
                    operation: text(row, 1)?,
                    status: text(row, 2)?,
                    pre_check: row.try_get::<&str, _>(3)?.map(str::to_owned),
                })
            })
            .transpose()
    }

    pub async fn start_run(&self, db: &mut Db, initial: bool) -> Result<RunContext> {
        let rows = db.query(&format!("UPDATE {} SET run_id=run_id+1 OUTPUT CAST(DELETED.run_id AS bigint) WHERE enabled=1 AND environment=@P1", self.table("run$cfg")?),
            &[self.settings.config.environment.as_str().into()]).await?;
        ensure!(
            rows.len() == 1,
            "run$cfg must have exactly one enabled row for this environment"
        );
        let ctx = RunContext {
            run_id: number(&rows[0], 0)?,
            operation: self.settings.family.operation(initial),
        };
        let params = vec![
            ctx.run_id.into(),
            self.settings.config.environment.as_str().into(),
            ctx.operation.as_str().into(),
            self.settings.family.data().into(),
            self.settings.target_table.as_str().into(),
            Value::Text(self.settings.config.pipeline_run_id.clone()),
            i64::try_from(self.settings.config.files_per_worker)?.into(),
            i64::try_from(self.settings.concurrency)?.into(),
            self.settings.config.input_folder.as_str().into(),
            self.settings.config.drop_id.into(),
        ];
        db.exec(&format!("INSERT INTO {} (run_id, environment, operation, operation_data, operation_cat, target_table, pipeline_run_id, cfg_files_per_worker, cfg_concurrency, start_timestamp, status, restart_count, rows_read, db_rows_loaded, files_loaded, files_failed, workers_failed, total_files_in_source, total_files_loaded, total_files_failed, total_elapsed_seconds, Input_Source_Folder, [drop]) VALUES (@P1,@P2,@P3,@P4,'Load',@P5,@P6,@P7,@P8,{NOW},'STARTED',0,0,0,0,0,0,0,0,0,0,@P9,@P10)", self.table("run$log")?), &params).await?;
        Ok(ctx)
    }

    pub async fn restart_run(&self, db: &mut Db, ctx: &RunContext) -> Result<()> {
        db.exec(&format!("UPDATE {} SET status='STARTED', restart_timestamp={NOW}, end_timestamp=NULL, restart_count=COALESCE(restart_count,0)+1, pipeline_run_id=@P1, pre_check=CASE WHEN pre_check IN ('FAIL','ERROR') THEN NULL ELSE pre_check END, post_check=NULL WHERE run_id=@P2 AND environment=@P3 AND operation=@P4", self.table("run$log")?),
            &[Value::Text(self.settings.config.pipeline_run_id.clone()), ctx.run_id.into(), self.settings.config.environment.as_str().into(), ctx.operation.as_str().into()]).await?;
        Ok(())
    }

    pub async fn message(&self, db: &mut Db, run_id: i64, message: &str) -> Result<()> {
        db.exec(
            &format!(
                "INSERT INTO {} (run_id,created_timestamp,message) VALUES (@P1,{NOW},@P2)",
                self.table("run$message")?
            ),
            &[
                run_id.into(),
                message.chars().take(3500).collect::<String>().into(),
            ],
        )
        .await?;
        Ok(())
    }

    pub async fn check_status(
        &self,
        db: &mut Db,
        ctx: &RunContext,
        phase: &str,
        status: &str,
        message: &str,
    ) -> Result<()> {
        ensure!(
            ["pre_check", "post_check"].contains(&phase),
            "Invalid check phase"
        );
        db.exec(&format!("UPDATE {} SET {phase}=@P1, messages=CONCAT(@P2,' | ',COALESCE(messages,'')) WHERE run_id=@P3 AND environment=@P4 AND operation=@P5", self.table("run$log")?),
            &[status.into(), message.into(), ctx.run_id.into(), self.settings.config.environment.as_str().into(), ctx.operation.as_str().into()]).await?;
        self.message(db, ctx.run_id, message).await
    }

    pub async fn expected(&self, db: &mut Db, phase: &str, rows: bool) -> Result<Option<i64>> {
        let column = if !rows {
            "file_count"
        } else if self.settings.config.data_repair {
            "post_data_repair_row_count"
        } else {
            "row_count"
        };
        let result = db.query(&format!("SELECT CAST({column} AS bigint) FROM {} WHERE target_table=@P1 AND [type]=@P2 AND environment=@P3", self.table("run$check")?),
            &[self.settings.target_table.as_str().into(), phase.into(), self.settings.config.environment.as_str().into()]).await?;
        ensure!(
            result.len() <= 1,
            "Multiple run$check rows match this target, phase, and environment"
        );
        let Some(row) = result.first() else {
            return Ok(None);
        };
        let value = row.try_get::<i64, _>(0)?;
        Ok(value.filter(|v| *v >= 0))
    }

    pub async fn sql_check(&self, db: &mut Db, ctx: &RunContext, phase: &str) -> Result<bool> {
        let checks: serde_json::Value = serde_json::from_str(include_str!("../sql/checks.json"))?;
        let check = &checks[&ctx.operation][phase];
        ensure!(
            !check.is_null(),
            "Missing check configuration for {}",
            ctx.operation
        );
        let column = if phase == "PRE_CHECK" {
            "pre_check"
        } else {
            "post_check"
        };
        let Some(file) = check["file"].as_str() else {
            self.check_status(
                db,
                ctx,
                column,
                "PASS",
                &format!("{phase}: no T-SQL defined in the source notebook"),
            )
            .await?;
            return Ok(true);
        };
        let sql = crate::sql_scripts::script(file)
            .context("Missing embedded SQL script")?
            .replace("{SCHEMA}", &quote_identifier(&self.settings.schema)?)
            .replace("{TARGET_TABLE}", &quote_table(&self.settings.target_table)?)
            .replace(
                "{ENVIRONMENT}",
                &self.settings.config.environment.replace('\'', "''"),
            );
        let log_rows = db.query(&format!("INSERT INTO {} (parent_id,operation,environment,label,start_timestamp,status,stmt) OUTPUT CAST(INSERTED.sql_id AS bigint) VALUES (@P1,'Load',@P2,@P3,{NOW},'RUNNING',@P4)", self.table("run$sql_log")?),
            &[ctx.run_id.into(), self.settings.config.environment.as_str().into(), phase.into(), sql.chars().take(4000).collect::<String>().into()]).await?;
        let sql_id = number(log_rows.first().context("No SQL log ID")?, 0)?;
        self.check_status(db, ctx, column, "In_Progress", &format!("Running {phase}"))
            .await?;
        let result = db.query(&sql, &[]).await;
        let (status, message, pass) = match &result {
            Ok(rows) => {
                let row = rows.last().context("Check SQL returned no result row")?;
                let actual = number(row, 0)?;
                let expected = check["expected"]
                    .as_i64()
                    .context("Missing expected check result")?;
                (
                    "COMPLETED",
                    row.try_get::<&str, _>(1)?
                        .unwrap_or("Check completed")
                        .to_owned(),
                    actual == expected,
                )
            }
            Err(error) => ("ERRORED", format!("{error:#}"), false),
        };
        db.exec(&format!("UPDATE {} SET end_timestamp={NOW},status=@P1,actual_rows_impacted=0,messages=@P2 WHERE parent_id=@P3 AND sql_id=@P4", self.table("run$sql_log")?),
            &[status.into(), message.chars().take(4000).collect::<String>().into(), ctx.run_id.into(), sql_id.into()]).await?;
        self.check_status(
            db,
            ctx,
            column,
            if pass {
                "PASS"
            } else if result.is_err() {
                "ERROR"
            } else {
                "FAIL"
            },
            &message,
        )
        .await?;
        result?;
        Ok(pass)
    }

    pub async fn file_record(
        &self,
        db: &mut Db,
        ctx: &RunContext,
        relative: &str,
        lock: bool,
    ) -> Result<Option<FileRecord>> {
        let lock = if lock { "WITH (UPDLOCK,HOLDLOCK)" } else { "" };
        let rows = db.query(&format!("SELECT status, CAST(COALESCE(rows_read,0) AS bigint), file_checksum FROM {} {lock} WHERE run_id=@P1 AND environment=@P2 AND operation=@P3 AND file_name=@P4", self.table("audit$file")?), &self.key(ctx, relative)).await?;
        ensure!(rows.len() <= 1, "Duplicate audit records for this file");
        rows.first()
            .map(|row| {
                Ok(FileRecord {
                    status: text(row, 0)?,
                    rows: number(row, 1)?,
                    checksum: row.try_get::<&str, _>(2)?.map(str::to_owned),
                })
            })
            .transpose()
    }

    /// Start a file transaction. An audit SUCCESS and its data are committed together.
    pub async fn begin_file(
        &self,
        db: &mut Db,
        ctx: &RunContext,
        relative: &str,
        basename: &str,
        size: u64,
    ) -> Result<Option<FileRecord>> {
        db.exec("SET XACT_ABORT ON; BEGIN TRANSACTION;", &[])
            .await?;
        let record = self.file_record(db, ctx, relative, true).await?;
        if let Some(record) = record.filter(|r| r.status == "SUCCESS") {
            db.exec("COMMIT TRANSACTION;", &[]).await?;
            return Ok(Some(record));
        }
        let mut params = self.key(ctx, relative);
        params.extend([basename.into(), i64::try_from(size)?.into()]);
        db.exec(&format!("IF EXISTS(SELECT 1 FROM {} WHERE run_id=@P1 AND environment=@P2 AND operation=@P3 AND file_name=@P4) UPDATE {} SET status='IN_PROGRESS',end_timestamp=NULL,error_message=NULL,retry_count=COALESCE(retry_count,0)+1,retry_start_timestamp={NOW},file_size_bytes=@P6 WHERE run_id=@P1 AND environment=@P2 AND operation=@P3 AND file_name=@P4; ELSE INSERT INTO {} (run_id,environment,operation,file_name,base_file_name,file_size_bytes,start_timestamp,status,retry_count) VALUES (@P1,@P2,@P3,@P4,@P5,@P6,{NOW},'IN_PROGRESS',0);", self.table("audit$file")?, self.table("audit$file")?, self.table("audit$file")?), &params).await?;
        Ok(None)
    }

    pub async fn checksum(
        &self,
        db: &mut Db,
        ctx: &RunContext,
        relative: &str,
        checksum: &str,
    ) -> Result<()> {
        let mut params = self.key(ctx, relative);
        params.push(checksum.into());
        db.exec(&format!("UPDATE {} SET file_checksum=@P5 WHERE run_id=@P1 AND environment=@P2 AND operation=@P3 AND file_name=@P4", self.table("audit$file")?), &params).await?;
        Ok(())
    }

    pub async fn complete_file(
        &self,
        db: &mut Db,
        ctx: &RunContext,
        relative: &str,
        rows: u64,
    ) -> Result<()> {
        let mut params = self.key(ctx, relative);
        params.push(i64::try_from(rows)?.into());
        db.exec(&format!("UPDATE {} SET status='SUCCESS',end_timestamp={NOW},rows_read=@P5,error_message=NULL WHERE run_id=@P1 AND environment=@P2 AND operation=@P3 AND file_name=@P4; COMMIT TRANSACTION;", self.table("audit$file")?), &params).await?;
        Ok(())
    }

    /// Return false if an ambiguous commit actually succeeded. Never turn SUCCESS into FAILED.
    pub async fn fail_file(
        &self,
        db: &mut Db,
        ctx: &RunContext,
        relative: &str,
        basename: &str,
        size: u64,
        error: &str,
    ) -> Result<bool> {
        db.exec("SET XACT_ABORT ON; BEGIN TRANSACTION;", &[])
            .await?;
        if self
            .file_record(db, ctx, relative, true)
            .await?
            .is_some_and(|r| r.status == "SUCCESS")
        {
            db.exec("COMMIT TRANSACTION;", &[]).await?;
            return Ok(false);
        }
        let mut params = self.key(ctx, relative);
        params.extend([
            basename.into(),
            i64::try_from(size)?.into(),
            error.chars().take(3500).collect::<String>().into(),
        ]);
        db.exec(&format!("IF EXISTS(SELECT 1 FROM {} WHERE run_id=@P1 AND environment=@P2 AND operation=@P3 AND file_name=@P4) UPDATE {} SET status='FAILED',end_timestamp={NOW},error_message=@P7 WHERE run_id=@P1 AND environment=@P2 AND operation=@P3 AND file_name=@P4; ELSE INSERT INTO {} (run_id,environment,operation,file_name,base_file_name,file_size_bytes,start_timestamp,end_timestamp,status,retry_count,error_message) VALUES (@P1,@P2,@P3,@P4,@P5,@P6,{NOW},{NOW},'FAILED',0,@P7); COMMIT TRANSACTION;", self.table("audit$file")?, self.table("audit$file")?, self.table("audit$file")?), &params).await?;
        Ok(true)
    }

    pub async fn audit_names(&self, db: &mut Db, ctx: &RunContext) -> Result<HashSet<String>> {
        let rows = db.query(&format!("SELECT file_name FROM {} WHERE run_id=@P1 AND environment=@P2 AND operation=@P3", self.table("audit$file")?),
            &[ctx.run_id.into(), self.settings.config.environment.as_str().into(), ctx.operation.as_str().into()]).await?;
        rows.iter().map(|r| text(r, 0)).collect()
    }

    pub async fn stats(&self, db: &mut Db, ctx: &RunContext) -> Result<Stats> {
        let rows = db.query(&format!("SELECT COALESCE(SUM(CASE WHEN status='SUCCESS' THEN CAST(1 AS bigint) ELSE 0 END),0),COALESCE(SUM(CASE WHEN status='FAILED' THEN CAST(1 AS bigint) ELSE 0 END),0),COALESCE(SUM(CASE WHEN status NOT IN ('SUCCESS','FAILED') THEN CAST(1 AS bigint) ELSE 0 END),0),COALESCE(SUM(CASE WHEN status='SUCCESS' THEN CAST(COALESCE(rows_read,0) AS bigint) ELSE 0 END),0) FROM {} WHERE run_id=@P1 AND environment=@P2 AND operation=@P3", self.table("audit$file")?),
            &[ctx.run_id.into(), self.settings.config.environment.as_str().into(), ctx.operation.as_str().into()]).await?;
        let row = rows.first().context("No audit statistics")?;
        Ok(Stats {
            files_loaded: number(row, 0)?,
            files_failed: number(row, 1)?,
            files_pending: number(row, 2)?,
            rows_loaded: number(row, 3)?,
        })
    }

    pub async fn row_count(&self, db: &mut Db, table: &str, run_id: i64) -> Result<i64> {
        let rows = db
            .query(
                &format!(
                    "SELECT COUNT_BIG(*) FROM {} WHERE x_run_id=@P1",
                    quote_table(table)?
                ),
                &[run_id.into()],
            )
            .await?;
        number(rows.first().context("No row count")?, 0)
    }

    pub async fn progress(
        &self,
        db: &mut Db,
        ctx: &RunContext,
        total: i64,
        stats: &Stats,
    ) -> Result<()> {
        db.exec(&format!("UPDATE {} SET files_in_source=@P1,files_loaded=@P2,files_failed=@P3,rows_read=@P4 WHERE run_id=@P5 AND environment=@P6 AND operation=@P7", self.table("run$log")?),
            &[total.into(), stats.files_loaded.into(), stats.files_failed.into(), stats.rows_loaded.into(),ctx.run_id.into(),self.settings.config.environment.as_str().into(),ctx.operation.as_str().into()]).await?;
        Ok(())
    }

    pub async fn finish(
        &self,
        db: &mut Db,
        ctx: &RunContext,
        completion: Completion<'_>,
    ) -> Result<()> {
        let Completion {
            status,
            total,
            stats,
            db_rows,
            message,
        } = completion;
        self.progress(db, ctx, total, stats).await?;
        db.exec(&format!("UPDATE {} SET status=@P1,end_timestamp={NOW},db_rows_loaded=@P2,messages=CONCAT(@P3,' | ',COALESCE(messages,'')),total_files_loaded=COALESCE(total_files_loaded,0)+@P4,total_files_failed=COALESCE(total_files_failed,0)+@P5,total_files_in_source=COALESCE(total_files_in_source,0)+@P6,total_elapsed_seconds=COALESCE(total_elapsed_seconds,0)+DATEDIFF(SECOND,COALESCE(restart_timestamp,start_timestamp),{NOW}) WHERE run_id=@P7 AND environment=@P8 AND operation=@P9", self.table("run$log")?),
            &[status.into(),db_rows.into(),message.chars().take(3500).collect::<String>().into(),stats.files_loaded.into(),stats.files_failed.into(),total.into(),ctx.run_id.into(),self.settings.config.environment.as_str().into(),ctx.operation.as_str().into()]).await?;
        if !message.is_empty() {
            self.message(db, ctx.run_id, message).await?;
        }
        Ok(())
    }

    pub async fn checkpoint(&self, db: &mut Db) -> Result<bool> {
        let rows = db.query(&format!("WITH latest AS (SELECT operation_data,status,ROW_NUMBER() OVER(PARTITION BY operation_data ORDER BY run_id DESC) AS rn FROM {} WHERE environment=@P1 AND operation_data IN ('GL','AR','AR_RECEIPT','TB','BU_CUSTOMER','JOURNAL_BATCH')) SELECT CASE WHEN COUNT(*)=6 AND SUM(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END)=6 THEN 1 ELSE 0 END FROM latest WHERE rn=1", self.table("run$log")?),
            &[self.settings.config.environment.as_str().into()]).await?;
        Ok(number(rows.first().context("No checkpoint result")?, 0)? == 1)
    }
}

impl Db {
    pub async fn query(&mut self, sql: &str, params: &[Value]) -> Result<Vec<Row>> {
        let mut query = Query::new(sql);
        for param in params {
            match param {
                Value::Int(n) => query.bind(*n),
                Value::Text(s) => query.bind(s.as_deref()),
            }
        }
        // Drain all result sets, including DDL and stored-procedure result sets.
        Ok(query
            .query(&mut self.client)
            .await?
            .into_results()
            .await?
            .into_iter()
            .flatten()
            .collect())
    }
    pub async fn exec(&mut self, sql: &str, params: &[Value]) -> Result<u64> {
        let mut query = Query::new(sql);
        for param in params {
            match param {
                Value::Int(n) => query.bind(*n),
                Value::Text(s) => query.bind(s.as_deref()),
            }
        }
        Ok(query.execute(&mut self.client).await?.total())
    }
    pub async fn insert(
        &mut self,
        table: &str,
        columns: &[String],
        rows: &[DataRow],
    ) -> Result<()> {
        ensure!(!columns.is_empty(), "Cannot insert zero columns");
        let width = columns.len();
        ensure!(
            width <= 2000,
            "Too many columns for SQL Server's parameter limit"
        );
        let names = columns
            .iter()
            .map(|s| quote_identifier(s))
            .collect::<Result<Vec<_>>>()?
            .join(",");
        let batch = (2000 / width).min(1000);
        for rows in rows.chunks(batch) {
            ensure!(
                rows.iter().all(|r| r.len() == width),
                "Data row width differs from header"
            );
            let values = (0..rows.len())
                .map(|r| {
                    format!(
                        "({})",
                        (0..width)
                            .map(|c| format!("@P{}", r * width + c + 1))
                            .collect::<Vec<_>>()
                            .join(",")
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            let sql = format!(
                "INSERT INTO {} ({names}) VALUES {values}",
                quote_table(table)?
            );
            let mut query = Query::new(sql);
            for row in rows {
                for value in row {
                    query.bind(value.as_deref());
                }
            }
            query.execute(&mut self.client).await?;
        }
        Ok(())
    }
}
