use fa_load::config::{Config, Family};

#[test]
fn all_notebook_operation_families_resolve_in_both_environments() {
    for environment in ["Live", "Test"] {
        for operation in [
            "GL_Load",
            "TB_Load",
            "AR_Load",
            "AR_RECEIPT_Load",
            "BU_CUSTOMER_Load",
            "JOURNAL_BATCH_Load",
        ] {
            let config = Config {
                environment: environment.into(),
                operation: operation.into(),
                ..Config::default()
            }
            .resolve()
            .unwrap();
            assert_eq!(config.family, Family::parse(operation).unwrap());
            assert_eq!(config.config.files_per_worker, 16);
            assert!(config.target_table.starts_with(if environment == "Live" {
                "fa."
            } else {
                "fa_test."
            }));
        }
    }
}

#[test]
fn bundled_configuration_matches_the_original_test_target() {
    let config: Config = toml::from_str(include_str!("../config.example.toml")).unwrap();
    let config = config.resolve().unwrap();
    assert_eq!(config.target_table, "fa_test.trialrun12_gl_raw");
    assert_eq!(config.concurrency, 4);
}

#[test]
fn reconciliation_rejects_incomplete_data_and_metadata() {
    use fa_load::{db::Stats, orchestrator::reconcile};
    let stats = Stats {
        files_loaded: 2,
        rows_loaded: 10,
        ..Stats::default()
    };
    assert!(reconcile(&stats, 2, 10, Some(10), Some(2)).is_ok());
    assert!(reconcile(&stats, 2, 10, None, Some(1)).is_err());
    assert!(reconcile(&stats, 2, 9, None, None).is_err());
    assert!(reconcile(&stats, 3, 10, None, None).is_err());
}
