# Fund accounting loader in Rust

This project converts the three supplied Spark/Fabric notebooks into a standalone asynchronous Rust CLI using **Tokio**. Azure storage transfers, SQL connections, worker execution, retries, and timers are asynchronous. CSV and Excel parsing run through `tokio::task::spawn_blocking`, connected to SQL writers by bounded channels.

| Notebook | Rust entry point | Main implementation |
|---|---|---|
| `fa-load-orchestrator.ipynb` | `fa-load orchestrate` | `src/orchestrator.rs` |
| `fa-load-worker.ipynb` | `fa-load worker` | `src/worker.rs`, `src/parser.rs` |
| `fa-load-tb-worker.ipynb` | `fa-load tb-worker` | `src/worker.rs`, `src/parser.rs` |

The default environment, storage account, database, and target tables come from the notebooks. Review `config.example.toml` and select your intended environment and operation before running a load. The original notebooks have not been changed.

## Build and run

Use Rust 1.85 or later with the usual compiler/linker prerequisites for your platform. On Windows, a standard Rust MSVC installation requires the Visual Studio C++ build tools; a GNU installation requires MinGW when building the TLS dependencies. On Linux, install a C compiler and standard build tools.

```powershell
cargo build --locked --release
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
Copy-Item config.example.toml config.local.toml
```

For development with an existing Azure CLI login:

```powershell
az login
.\target\release\fa-load.exe --config config.local.toml orchestrate
```

For service-principal authentication, set `auth.mode = "service_principal"` and supply `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, and `AZURE_CLIENT_SECRET` through your execution environment or secret manager. No secret is stored in the example configuration. For a VM using Azure Instance Metadata Service, select `managed_identity`; optionally set `managed_identity_client_id` for a user-assigned identity. App Service/Container Apps identity endpoints and sovereign Azure clouds need an authentication adapter.

Inspect a real input locally without Azure or SQL credentials:

```powershell
.\target\release\fa-load.exe inspect --kind csv --path 'C:\data\input.csv'
.\target\release\fa-load.exe inspect --kind tb --path 'C:\data\Trial Balance Report Apr-26.xlsx'
```

Inspection prints columns, exact row count, the first three rows, and Excel metadata. It is useful for validating your actual files before a database load.

The worker commands accept the original `FilesJson` array, including ABFSS paths for the configured account/container, or a JSON manifest file. Workers require an existing matching `STARTED` row in `run$log`:

```powershell
.\target\release\fa-load.exe --config config.local.toml worker --run-id 2010 --operation Initial_GL_Load --files-file manifest.json
.\target\release\fa-load.exe --config config.local.toml tb-worker --run-id 2011 --operation Initial_TB_Load --files-file tb-manifest.json
```

Set the corresponding `operation` in configuration (`GL_Load`, `TB_Load`, and so on). Normally use `orchestrate`; it creates the run, launches workers, validates results, and moves completed files to `Pass`. Manual workers are for controlled integration with an existing orchestration process; avoid overlapping them with an active Rust orchestrator.

CLI summaries are JSON on stdout. Diagnostic logs go to stderr; `RUST_LOG=fa_load=debug` increases detail. Exit codes are `0` for success, `1` for a completed failed/blocked run, and `2` for execution/configuration errors. An orchestrated `ERRORED` summary exits `1`; an error preventing a summary exits `2`. `Pipeline_Checkpoint` returns normal CLI success/failure exit codes rather than Fabric notebook exit values.

## Load behavior

The orchestrator acquires a SQL session application lock for the environment, load family, and target table. It loads a sorted snapshot of matching input paths, with at most `concurrency` worker tasks per wave and `files_per_worker` sequential files per worker. Defaults preserve the notebooks: 16 files per worker; four GL workers; three Test TB workers/four Live TB workers; one worker for the other load families.

Each file is downloaded to a temporary disk spool while SHA-256 is calculated. CSV rows are streamed through bounded batches. Excel workbooks are read with Calamine; their sheets are materialized in memory. A file transaction atomically inserts data, inserts the single TB metadata record where applicable, and marks its audit row `SUCCESS`. SQL INSERT batches stay below SQL Server's 2,100-parameter and 1,000-VALUES-row limits. Source CSV values remain strings and empty fields become SQL NULL, as in the notebook's non-inferred CSV schema. Target column conversion is performed by SQL Server.

After commit, the file moves from `In` to `Out/<run_id>/<relative path>`. Failures exhaust the configured retries and are recorded in `audit$file`, then moved to `Error/<run_id>` if the input still matches the original storage snapshot. If a database commit succeeded but its archive move failed, the audit row remains successful and the file stays available for move repair. A restart checks the existing audit row and its SHA-256 before skipping an insert. Storage renames use source ETags and never overwrite an existing destination.

Successful files move from `Out/<run_id>` to `Pass/<run_id>` only after file/row reconciliation, expected-count checks, TB metadata checks, and the notebook's T-SQL checks pass. Failed files in `Error` must be corrected and returned to `In` before a restart can succeed. New files arriving after the input snapshot wait for a later run.

After finalisation, empty child directories beneath `In` are removed with nonrecursive deletes; nonempty directories and the input root are retained.

The latest successful run starts a new incremental run. Failed, errored, or blocked runs reuse their run ID and operation. A `STARTED` run requires `--resume-started` after you have confirmed the old process is stopped. Application locks coordinate Rust loaders; the original Python notebooks do not acquire these locks.

## Azure and database requirements

The storage account must support ADLS Gen2 hierarchical namespaces and the `dfs.core.windows.net` APIs. The identity needs data access and directory/file rename permissions, including applicable ACLs. OneLake/Fabric filesystem paths are not supported by this adapter. SQL connections use Entra tokens through Tiberius with TLS encryption and certificate validation enabled.

Existing SQL objects are required: `run$cfg`, `run$log`, `run$check`, `run$message`, `run$sql_log`, `audit$file`, the configured raw target, and `gl_tb_metadata` for TB. The notebook did not supply their DDL, so this project does not invent or recreate them. See `MIGRATION.md` for the column contract. The database identity also needs the index/statistics permissions used by the GL post-check SQL.

## Validation and operational limits

See `VALIDATION.md` for the checks actually run. Automated tests use synthetic CSV and real generated XLSX files. Azure authentication, storage APIs, target schema compatibility, SQL transaction recovery, and live file moves require validation in your own Test environment. No production/cloud load was executed during this conversion.

This single-process loader replaces Spark's distributed execution. Parameterized INSERT batches favor a clear transactional implementation and can be slower than optimized JDBC/bulk loading. Benchmark representative file sizes and concurrency before choosing production settings. Whole-file transactions also require enough SQL transaction-log capacity. Excel parsing can temporarily use several times the compressed workbook size; lower TB concurrency for large workbooks. The parser does not calculate Excel formulas; cached workbook values must be available.

CSV parsing accepts standard UTF-8 RFC 4180 quoting, multiline records, and doubled quotes. It rejects inconsistent record widths. This is stricter than Spark's default permissive CSV mode, and Spark's `escape=""` can interpret malformed/escaped input differently. Confirm representative exports with `inspect`. Raw load timestamps are UTC; audit SQL timestamps retain the notebook's `AUS Eastern Standard Time` conversion. `x_row_num` is deterministic and zero-based, replacing Spark's partition-dependent ID.

Deploy the CLI as a container/job/application and have your pipeline invoke it. Tokio does not provide a Fabric Spark notebook runtime or a Spark-compatible distributed engine. `data_repair` selects the expected post-repair row-count column; the source's inactive repair/debug code and commented clustered-index procedure call are not executed.

# fa-load
