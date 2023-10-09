use cli_test_dir::*;

#[test]
fn run_sql_tests() {
    let testdir = TestDir::new("joinery", "write_output_file");
    let sql_test_dir = testdir.src_path("tests/sql/");
    testdir
        .cmd()
        .arg("sql-test")
        .arg(&sql_test_dir)
        .expect_success();
}
