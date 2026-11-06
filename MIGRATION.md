# Conversion details

## Source evidence

`source-manifest.json` records SHA-256 hashes of the exact supplied notebooks and the extracted source locations for checks. `sql/checks.json` maps all twelve initial/incremental operations to their SQL. The ten non-empty SQL batches are embedded in the binary through `src/sql_scripts.rs`; files are also provided separately for review. The orchestrator's duplicate function definitions and commented development experiments were consolidated.

## Operation and target mappings

| Base operation | Live target | Test target |
|---|---|---|
| `GL_Load` | `fa.combined_gl_raw` | `fa_test.trialrun12_gl_raw` |
| `TB_Load` | `fa.combined_tb_raw` | `fa_test.drop2_tb_raw` |
| `AR_Load` | `fa.combined_ar_raw` | `fa_test.drop2_ar_raw` |
| `AR_RECEIPT_Load` | `fa.combined_ar_receipt_raw` | `fa_test.drop2_ar_receipt_raw` |
| `BU_CUSTOMER_Load` | `fa.combined_sas_customer_raw_extra` | `fa_test.drop2_sas_customer_raw_extra` |
| `JOURNAL_BATCH_Load` | `fa.combined_journal_batch_final_approvers_raw` | `fa_test.drop2_journal_batch_final_approvers_raw` |

`schema` and `target_table` can be overridden independently. Files match the notebook's path markers; GL excludes TB reports and journal batches, AR excludes receipts, customer matches `_BU_CUSTOMER_` or `_CUSTOMER_`. TB additionally accepts the text `Trial Balance Report` when an `_TB_` marker is absent.

## SQL column contract

| Object | Columns read or written |
|---|---|
| `run$cfg` | `run_id`, `enabled`, `environment` |
| `run$check` | `target_table`, `type`, `environment`, `file_count`, `row_count`, `post_data_repair_row_count` |
| `run$log` | `run_id`, `environment`, `operation`, `operation_data`, `operation_cat`, `target_table`, `pipeline_run_id`, `cfg_files_per_worker`, `cfg_concurrency`, `start_timestamp`, `restart_timestamp`, `end_timestamp`, `status`, `restart_count`, `pre_check`, `post_check`, `messages`, `rows_read`, `db_rows_loaded`, `files_loaded`, `files_failed`, `workers_failed`, `files_in_source`, `total_files_in_source`, `total_files_loaded`, `total_files_failed`, `total_elapsed_seconds`, `Input_Source_Folder`, `drop` |
| `audit$file` | `run_id`, `environment`, `operation`, `file_name`, `base_file_name`, `file_size_bytes`, `start_timestamp`, `end_timestamp`, `retry_start_timestamp`, `status`, `retry_count`, `rows_read`, `file_checksum`, `error_message` |
| `run$message` | `run_id`, `created_timestamp`, `message` |
| `run$sql_log` | `sql_id` (generated), `parent_id`, `operation`, `environment`, `label`, `start_timestamp`, `end_timestamp`, `status`, `stmt`, `actual_rows_impacted`, `messages` |
| CSV raw table | Every input header plus `x_drop`, `x_SourceFile`, `x_Run_Id`, `x_LoadTimestamp` |
| TB raw table | `account`, `description`, `account_long`, `Begining_balance_AUD`, `Debits_AUD`, `Credits_AUD`, `Ending_Balance_AUD`, `Fund`, `Year`, `Month`, `x_row_num`, plus the four common raw load columns |
| `gl_tb_metadata` | `x_drop`, `x_run_id`, `environment`, `x_fund`, `x_year`, `x_month`, `x_sourcefile`, `company_name_sheet1`, `report_date_sheet1`, `ledger_or_ledger_set_sheet1`, `currency_type`, `ledger_currency`, `accounting_period`, `amount_type`, `summarize_by`, optional `balancing_segment`, `report_date_sheet2`, `company_name_sheet2`, `ledger_sheet2`, `page_sheet2`, `fund_sheet2`, `beginning_balance_aud_total`, `debits_aud_total`, `credits_aud_total`, `ending_balance_aud_total` |

Column spelling, including `Begining`, preserves the source. Existing targets normally use case-insensitive SQL identifier collation; confirm names in a database using case-sensitive collation. SQL Server implicitly converts bound strings into numeric/date columns. Raw amount columns should accept the original formatted strings, including commas. Missing optional metadata becomes NULL, except an absent balancing segment omits that column as in the notebook.

There must be a unique audit-file key for `(run_id, environment, operation, file_name)`, exactly one enabled `run$cfg` row per environment, and at most one `run$check` row per target/type/environment. Retain the database's existing checksum uniqueness constraints. Duplicate checksums fail and the SQL error is recorded in `error_message`; `dup_checksum` is not required by this implementation.

## Trial-balance mapping

Sheet1 metadata is read from A1:H14. The company/fund is taken from column A one row below `Report Date`, and accounting month/year from `Accounting Period` (`Mon-YY`, including Python's 1969/2068 year pivot). All six supplied fund mappings are retained. Sheet2 metadata is read from A1:J20; labels use the first subsequent nonblank cell. Its `ACCOUNT` header determines where data begins. Raw data selects columns A, B, D, F, G, I, and K, retaining nonblank report/footer rows as the source does. The final `Total` row containing four balances supplies the one metadata record.

## Intentional corrections

- Row counts come from actual inserted rows and committed audit records. The notebooks could add the previous successful file's count again after a failed/skipped file.
- File writes and their success audit are committed together. A failed file rolls back all raw and metadata rows, preventing duplicate inserts on a retry. This differs from the source's independent JDBC append and audit commits.
- File claims use SQL update/range locks. Successful records are skipped only after verifying the stored checksum, and an archive error cannot turn a committed file into a failed data load.
- Workers report failure when files fail. CLI/pipeline status reflects blocked, failed, or errored runs rather than returning unconditional success.
- Restart totals use the union of audit-file paths and the current input snapshot. Previously archived successes are counted once.
- New incremental runs execute their configured pre-check. Initial-load history must have `status='SUCCESS'`; the extracted incremental GL/AR/TB checks were updated accordingly.
- The misspelled `Incrementa_AR_Load` configuration key is normalized to `Incremental_AR_Load`.
- TB metadata row-count mismatch prevents success and promotion. The notebook set a metadata flag without reliably setting its run failure flag.
- Missing `Page` metadata is handled as NULL; the notebook could index a missing row. Data start and total-row scans use the complete Sheet2 rather than unreliable row-count-derived Excel coordinates.
- Checkpoint queries filter by environment and order by run ID, so another environment's result cannot satisfy the checkpoint.
- All SQL data values are bound parameters; dynamic identifiers are escaped. Storage moves validate relative paths, ETags, and destinations.
- Parser channels are bounded. CSV parsing stops when its consumer drops; Excel sheet decompression cannot be interrupted until Calamine returns to the producer loop.

The standalone project uses a consistent UTC raw timestamp, deterministic `x_row_num`, strict CSV row widths, and no fixed two-second sleep. The inactive `usp_build_clustered_index` call remains inactive; the active GL SQL still creates `dup_idx` and updates statistics. Operations whose source check SQL was empty retain an empty check, with counts still reconciled.

## Migration boundaries

Before resuming a failed run originally written by Python, reconcile any partially committed JDBC data. Rust's transactional guarantees cover files loaded by this implementation; it cannot infer or automatically remove partial rows from older notebook attempts. Never run the Python and Rust loaders against the same operation concurrently.

Temporary download spools require local disk capacity. CSV memory use is bounded by batch and channel settings; Excel memory use depends on workbook size. Database/storage errors can prevent final audit updates if the service is unavailable. In that case, recover a stale `STARTED` run explicitly after confirming no process owns it.

