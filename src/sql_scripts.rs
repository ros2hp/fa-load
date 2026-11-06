// Extracted from the supplied orchestrator; see MIGRATION.md for intentional fixes.
pub fn script(name: &str) -> Option<&'static str> {
    Some(match name {
        "post_check_incremental_gl_load.sql" => {
            include_str!("../sql/post_check_incremental_gl_load.sql")
        }
        "post_check_incremental_tb_load.sql" => {
            include_str!("../sql/post_check_incremental_tb_load.sql")
        }
        "post_check_initial_gl_load.sql" => include_str!("../sql/post_check_initial_gl_load.sql"),
        "post_check_initial_tb_load.sql" => include_str!("../sql/post_check_initial_tb_load.sql"),
        "pre_check_incremental_ar_load.sql" => {
            include_str!("../sql/pre_check_incremental_ar_load.sql")
        }
        "pre_check_incremental_gl_load.sql" => {
            include_str!("../sql/pre_check_incremental_gl_load.sql")
        }
        "pre_check_incremental_tb_load.sql" => {
            include_str!("../sql/pre_check_incremental_tb_load.sql")
        }
        "pre_check_initial_ar_load.sql" => include_str!("../sql/pre_check_initial_ar_load.sql"),
        "pre_check_initial_gl_load.sql" => include_str!("../sql/pre_check_initial_gl_load.sql"),
        "pre_check_initial_tb_load.sql" => include_str!("../sql/pre_check_initial_tb_load.sql"),
        _ => return None,
    })
}
