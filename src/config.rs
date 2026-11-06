use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Family {
    Gl,
    Tb,
    Ar,
    ArReceipt,
    Customer,
    JournalBatch,
    Checkpoint,
}

impl Family {
    pub fn parse(operation: &str) -> Result<Self> {
        let upper = operation.to_ascii_uppercase();
        let base = upper
            .strip_prefix("INITIAL_")
            .or_else(|| upper.strip_prefix("INCREMENTAL_"))
            .or_else(|| upper.strip_prefix("INCREMENTA_"))
            .unwrap_or(&upper);
        Ok(match base {
            "GL_LOAD" => Self::Gl,
            "TB_LOAD" => Self::Tb,
            "AR_LOAD" => Self::Ar,
            "AR_RECEIPT_LOAD" => Self::ArReceipt,
            "BU_CUSTOMER_LOAD" => Self::Customer,
            "JOURNAL_BATCH_LOAD" => Self::JournalBatch,
            "PIPELINE_CHECKPOINT" => Self::Checkpoint,
            _ => bail!("Unsupported operation: {operation}"),
        })
    }

    pub fn data(self) -> &'static str {
        match self {
            Self::Gl => "GL",
            Self::Tb => "TB",
            Self::Ar => "AR",
            Self::ArReceipt => "AR_RECEIPT",
            Self::Customer => "BU_CUSTOMER",
            Self::JournalBatch => "JOURNAL_BATCH",
            Self::Checkpoint => "Pipeline_Checkpoint",
        }
    }

    pub fn operation(self, initial: bool) -> String {
        format!(
            "{}_{}_Load",
            if initial { "Initial" } else { "Incremental" },
            self.data()
        )
    }

    pub fn matches_path(self, path: &str) -> bool {
        let text = path.to_ascii_lowercase();
        match self {
            Self::Tb => text.contains("_tb_") || text.contains("trial balance report"),
            Self::Gl => {
                text.contains("_gl_")
                    && !Self::Tb.matches_path(path)
                    && !Self::JournalBatch.matches_path(path)
            }
            Self::ArReceipt => text.contains("_ar_receipt_"),
            Self::Ar => text.contains("_ar_") && !text.contains("_ar_receipt_"),
            Self::Customer => text.contains("_bu_customer_") || text.contains("_customer_"),
            Self::JournalBatch => text.contains("_journal_batch_"),
            Self::Checkpoint => false,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub environment: String,
    pub operation: String,
    pub drop_id: i64,
    pub input_folder: String,
    pub data_repair: bool,
    pub files_per_worker: usize,
    pub concurrency: Option<usize>,
    pub schema: Option<String>,
    pub target_table: Option<String>,
    pub pipeline_run_id: Option<String>,
    pub batch_rows: usize,
    pub parser_queue_batches: usize,
    pub file_timeout_seconds: u64,
    pub wave_timeout_seconds: u64,
    pub worker_retries: u32,
    pub retry_interval_seconds: u64,
    pub storage: StorageConfig,
    pub sql: SqlConfig,
    pub auth: AuthConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            environment: "Test".into(),
            operation: "GL_Load".into(),
            drop_id: 1,
            input_folder: "In".into(),
            data_repair: false,
            files_per_worker: 16,
            concurrency: None,
            schema: None,
            target_table: None,
            pipeline_run_id: None,
            batch_rows: 10_000,
            parser_queue_batches: 2,
            file_timeout_seconds: 3600,
            wave_timeout_seconds: 43_200,
            worker_retries: 1,
            retry_interval_seconds: 30,
            storage: StorageConfig::default(),
            sql: SqlConfig::default(),
            auth: AuthConfig::default(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageConfig {
    pub account: String,
    pub container: String,
    pub base_folder: String,
    pub connect_timeout_seconds: u64,
    pub read_timeout_seconds: u64,
}
impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            account: "stcstgmigae01".into(),
            container: "2-fund-accounting".into(),
            base_folder: "2-input/load".into(),
            connect_timeout_seconds: 10,
            read_timeout_seconds: 30,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SqlConfig {
    pub server: String,
    pub database: String,
    pub port: u16,
    pub connect_timeout_seconds: u64,
}
impl Default for SqlConfig {
    fn default() -> Self {
        Self {
            server: "stc-dev-db-mig.database.windows.net".into(),
            database: "stc-dev-core-ae".into(),
            port: 1433,
            connect_timeout_seconds: 30,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AuthConfig {
    pub mode: String,
    pub managed_identity_client_id: Option<String>,
}
impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            mode: "cli".into(),
            managed_identity_client_id: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Resolved {
    pub config: Config,
    pub family: Family,
    pub schema: String,
    pub target_table: String,
    pub concurrency: usize,
}

impl Config {
    pub fn read(path: &Path) -> Result<Self> {
        toml::from_str(&std::fs::read_to_string(path).context("Reading configuration")?)
            .context("Parsing configuration")
    }

    pub fn resolve(self) -> Result<Resolved> {
        let family = Family::parse(&self.operation)?;
        ensure!(
            self.files_per_worker > 0 && self.batch_rows > 0 && self.parser_queue_batches > 0,
            "Worker and batch sizes must be positive"
        );
        ensure!(
            self.file_timeout_seconds > 0 && self.wave_timeout_seconds > 0,
            "Timeouts must be positive"
        );
        ensure!(
            ["cli", "service_principal", "managed_identity"].contains(&self.auth.mode.as_str()),
            "auth.mode must be cli, service_principal, or managed_identity"
        );
        ensure!(
            !self.environment.is_empty() && !self.environment.contains(['/', '\\']),
            "Invalid environment"
        );
        validate_path(&self.input_folder)?;
        validate_path(&self.storage.base_folder)?;
        ensure!(
            self.storage
                .account
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
                && !self.storage.account.is_empty(),
            "Invalid storage account"
        );
        ensure!(
            !self.storage.container.is_empty()
                && self
                    .storage
                    .container
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'),
            "Invalid container"
        );
        let live = self.environment == "Live";
        let schema = self
            .schema
            .clone()
            .unwrap_or_else(|| if live { "fa" } else { "fa_test" }.into());
        quote_identifier(&schema)?;
        let suffix = match (live, family) {
            (true, Family::Gl) => "combined_gl_raw",
            (false, Family::Gl) => "trialrun12_gl_raw",
            (true, Family::Tb) => "combined_tb_raw",
            (false, Family::Tb) => "drop2_tb_raw",
            (true, Family::Ar) => "combined_ar_raw",
            (false, Family::Ar) => "drop2_ar_raw",
            (true, Family::ArReceipt) => "combined_ar_receipt_raw",
            (false, Family::ArReceipt) => "drop2_ar_receipt_raw",
            (true, Family::Customer) => "combined_sas_customer_raw_extra",
            (false, Family::Customer) => "drop2_sas_customer_raw_extra",
            (true, Family::JournalBatch) => "combined_journal_batch_final_approvers_raw",
            (false, Family::JournalBatch) => "drop2_journal_batch_final_approvers_raw",
            (_, Family::Checkpoint) => "unused",
        };
        let target_table = self
            .target_table
            .clone()
            .unwrap_or_else(|| format!("{schema}.{suffix}"));
        quote_table(&target_table)?;
        let concurrency = self.concurrency.unwrap_or(match family {
            Family::Gl => 4,
            Family::Tb if live => 4,
            Family::Tb => 3,
            _ => 1,
        });
        ensure!(
            concurrency > 0 && concurrency <= 128,
            "concurrency must be 1..128"
        );
        ensure!(
            concurrency.checked_mul(self.files_per_worker).is_some(),
            "Wave size overflow"
        );
        Ok(Resolved {
            config: self,
            family,
            schema,
            target_table,
            concurrency,
        })
    }
}

impl Resolved {
    pub fn folder(&self, leaf: &str) -> String {
        format!(
            "{}/{}/{}",
            self.config.storage.base_folder.trim_end_matches('/'),
            self.config.environment,
            leaf
        )
    }
    pub fn input(&self) -> String {
        self.folder(&self.config.input_folder)
    }
}

pub fn validate_path(path: &str) -> Result<()> {
    ensure!(
        !path.is_empty()
            && !path.starts_with('/')
            && !path.contains('\\')
            && path
                .split('/')
                .all(|s| !s.is_empty() && s != "." && s != "..")
            && !path.chars().any(char::is_control),
        "Invalid relative storage path: {path}"
    );
    Ok(())
}
pub fn quote_identifier(name: &str) -> Result<String> {
    ensure!(
        !name.is_empty() && name.chars().count() <= 128 && !name.chars().any(char::is_control),
        "Invalid SQL identifier"
    );
    Ok(format!("[{}]", name.replace(']', "]]")))
}
pub fn quote_table(name: &str) -> Result<String> {
    // The extracted check scripts also put table names inside OBJECT_ID/message literals.
    ensure!(
        !name.contains('\''),
        "Table identifiers containing apostrophes are unsupported"
    );
    let parts: Vec<_> = name.split('.').collect();
    ensure!(parts.len() == 2, "Target table must be schema.table");
    Ok(format!(
        "{}.{}",
        quote_identifier(parts[0])?,
        quote_identifier(parts[1])?
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn defaults_and_receipt_separation() {
        let cfg = Config::default().resolve().unwrap();
        assert_eq!(cfg.target_table, "fa_test.trialrun12_gl_raw");
        assert!(Family::ArReceipt.matches_path("folder_AR_RECEIPT_DATA_26/file.csv"));
        assert!(!Family::Ar.matches_path("folder_AR_RECEIPT_DATA_26/file.csv"));
        assert_eq!(Family::parse("Incrementa_AR_Load").unwrap(), Family::Ar);
    }
    #[test]
    fn path_and_identifier_validation() {
        assert!(validate_path("In/../Out").is_err());
        assert!(quote_table("db.schema.table").is_err());
        assert!(quote_table("schema.bad'name").is_err());
        assert_eq!(quote_identifier("a]b").unwrap(), "[a]]b]");
    }
}
