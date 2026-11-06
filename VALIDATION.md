# Validation

Validation performed on 15 October 2026 against the supplied notebook files.

| Check | Result |
|---|---|
| Rust compilation, including all targets | Passed |
| `cargo clippy --locked --all-targets -- -D warnings` | Passed, no warnings |
| `cargo fmt --check` | Passed |
| Automated tests | 13 passed, 0 failed |
| CLI `--help` | Passed |
| CLI `inspect` on the bundled CSV example | Passed; two rows, multiline text and doubled quotes preserved |
| SHA-256 of all three source notebooks | Unchanged |
| Check mappings | All 12 initial/incremental operations represented; 10 non-empty SQL scripts |

The tests cover operation/target mappings in Live and Test, receipt exclusion from AR, path containment and identifier escaping, CSV multiline records/quoting/NULLs/exact counts, invalid CSV widths and duplicate/audit headers, Python's accounting-year pivot and fund mappings, real generated XLSX files, missing Page metadata, trial-balance data after row 20, metadata totals and raw/footer rows, reconciliation failures, and bounded-parser cancellation.

The project was compiled with Rust 1.99.0 for `x86_64-pc-windows-gnu`, using an isolated workspace toolchain and LLVM linker/C build tools. No machine-wide Rust installation or PATH changes were made. Standard build commands are in README.md; platform/compiler prerequisites must be installed on the deployment host. Rust 1.85 is the declared minimum and dependency-resolution constraint, but it was not separately executed here.

Cloud execution was not performed. The supplied files contain notebook code rather than representative CSV/XLSX inputs or SQL table DDL. Azure identity permissions, TLS/network access to your services, the existing SQL schema and constraints, SQL transaction rollback/ambiguous-commit recovery, ADLS pagination/renames, live concurrency, checkpoint results, and throughput must be checked in your Test environment before production use. The tests do not claim to establish those behaviors against a live service.

`examples/sample-inspection.json` contains the actual CLI output for the bundled CSV example.
