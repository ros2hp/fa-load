use anyhow::{Context, Result, ensure};
use clap::{Parser, Subcommand, ValueEnum};
use fa_load::{
    auth::Tokens,
    config::{Config, Family},
    db::{Database, RunContext},
    orchestrator,
    parser::{self, Kind},
    storage::{InputFile, Storage},
};
use std::{path::PathBuf, sync::Arc};
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
#[command(
    version,
    about = "Async Azure fund-accounting loader converted from the supplied Spark notebooks"
)]
struct Cli {
    #[arg(long, default_value = "config.example.toml", global = true)]
    config: PathBuf,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Replaces fa-load-orchestrator, including Pipeline_Checkpoint.
    Orchestrate {
        #[arg(long)]
        resume_started: bool,
    },
    /// Replaces fa-load-worker; pass the same JSON file manifest as FilesJson.
    Worker(WorkerArgs),
    /// Replaces fa-load-tb-worker; configuration must select TB_Load.
    TbWorker(WorkerArgs),
    /// Inspect a local CSV or Excel file without connecting to Azure.
    Inspect {
        #[arg(long)]
        path: PathBuf,
        #[arg(long, value_enum)]
        kind: InspectKind,
    },
}
#[derive(clap::Args)]
struct WorkerArgs {
    #[arg(long)]
    run_id: i64,
    #[arg(long)]
    operation: String,
    #[arg(
        long,
        conflicts_with = "files_file",
        required_unless_present = "files_file"
    )]
    files_json: Option<String>,
    #[arg(long, conflicts_with = "files_json")]
    files_file: Option<PathBuf>,
    #[arg(long, default_value_t = 1)]
    wave_id: usize,
    #[arg(long, default_value_t = 1)]
    worker_id: usize,
}
#[derive(Clone, Copy, ValueEnum)]
enum InspectKind {
    Csv,
    Tb,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "fa_load=info".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    match execute(Cli::parse()).await {
        Ok(true) => {}
        Ok(false) => std::process::exit(1),
        Err(error) => {
            eprintln!("{error:#}");
            std::process::exit(2);
        }
    }
}

async fn execute(cli: Cli) -> Result<bool> {
    if let Command::Inspect { path, kind } = &cli.command {
        let output = parser::inspect(
            path,
            match kind {
                InspectKind::Csv => Kind::Csv,
                InspectKind::Tb => Kind::TrialBalance,
            },
        )
        .await?;
        println!("{}", serde_json::to_string_pretty(&output)?);
        return Ok(true);
    }
    let settings = Arc::new(Config::read(&cli.config)?.resolve()?);
    match &cli.command {
        Command::TbWorker(_) => ensure!(
            settings.family == Family::Tb,
            "tb-worker configuration must select TB_Load"
        ),
        Command::Worker(_) => ensure!(
            !matches!(settings.family, Family::Tb | Family::Checkpoint),
            "Use tb-worker for TB_Load"
        ),
        _ => {}
    }
    let tokens = Arc::new(Tokens::new(settings.config.auth.clone())?);
    let database = Database::new(settings.clone(), tokens.clone());
    let storage = Storage::new(settings.config.storage.clone(), tokens)?;
    let cancel = CancellationToken::new();
    let signal_cancel = cancel.clone();
    let signal = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            signal_cancel.cancel();
        }
    });
    let result = async {
        match cli.command {
            Command::Orchestrate { resume_started } => {
                let output = orchestrator::run(database, storage, resume_started, cancel).await?;
                println!("{}", serde_json::to_string_pretty(&output)?);
                Ok(output.status == "SUCCESS")
            }
            Command::Worker(args) | Command::TbWorker(args) => {
                let json = if let Some(json) = args.files_json {
                    json
                } else {
                    tokio::fs::read_to_string(args.files_file.context("Missing file manifest")?)
                        .await?
                };
                let files: Vec<InputFile> =
                    serde_json::from_str(&json).context("Parsing FilesJson")?;
                let output = orchestrator::manual_worker(
                    database,
                    storage,
                    RunContext {
                        run_id: args.run_id,
                        operation: args.operation,
                    },
                    files,
                    args.wave_id,
                    args.worker_id,
                    cancel,
                )
                .await?;
                println!("{}", serde_json::to_string_pretty(&output)?);
                Ok(output.status == "SUCCESS")
            }
            Command::Inspect { .. } => unreachable!(),
        }
    }
    .await;
    signal.abort();
    let _ = signal.await;
    result
}
