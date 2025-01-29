use assert_cmd::Command;
use std::fs;
use tempfile::tempdir;

const SAMPLE_CSV_DATA: &str = "\
name,age,city
Alice,30,NYC
Bob,25,SF
";

/// Converts `from` -> `to`, then `to` -> CSV, returning the final CSV string.
fn roundtrip_test_with_args(
    from: &str,
    to: &str,
    csv_data: &str,
    second_args: &[&str], // extra CLI args for the second command
) -> String {
    let tmp = tempdir().expect("Unable to create temp dir");
    let input_csv_path = tmp.path().join("input.csv");
    let intermediate_path = tmp.path().join(format!("intermediate.{to}"));
    let output_csv_path = tmp.path().join("output.csv");

    fs::write(&input_csv_path, csv_data).expect("Unable to write test CSV");

    Command::cargo_bin("frameblaze")
        .unwrap()
        .args([
            from, to, input_csv_path.to_str().unwrap(), "--output",
            intermediate_path.to_str().unwrap(),
        ])
        .assert()
        .success();

    let mut second_cmd_args = vec![
        to, "csv", intermediate_path.to_str().unwrap(), "--output",
        output_csv_path.to_str().unwrap(),
    ];
    second_cmd_args.extend_from_slice(second_args);

    Command::cargo_bin("frameblaze")
        .unwrap()
        .args(&second_cmd_args)
        .assert()
        .success();

    fs::read_to_string(output_csv_path).expect("Unable to read output CSV")
}

/// Asserts that all of the given `lines` appear somewhere in `csv_output`.
fn assert_csv_contains_lines(csv_output: &str, lines: &[&str]) {
    for line in lines {
        assert!(
            csv_output.contains(line),
            "Expected CSV to contain line {:?}, but it was missing.\n\nFull CSV:\n{}",
            line,
            csv_output
        );
    }
}

/// Asserts that none of the given `lines` appear in `csv_output`.
fn assert_csv_excludes_lines(csv_output: &str, lines: &[&str]) {
    for line in lines {
        assert!(
            !csv_output.contains(line),
            "Did NOT expect CSV to contain line {:?}, but it was found.\n\nFull CSV:\n{}",
            line,
            csv_output
        );
    }
}

#[test]
fn test_csv_to_parquet_roundtrip_with_exclude() {
    let result_csv = roundtrip_test_with_args(
        "csv",
        "parquet",
        SAMPLE_CSV_DATA,
        &["--exclude-columns", "city"],
    );
    assert_csv_contains_lines(
        &result_csv,
        &["name,age", "Alice,30", "Bob,25"],
    );
    assert_csv_excludes_lines(&result_csv, &["city"]);
}

#[test]
fn test_csv_to_parquet_roundtrip_with_include() {
    let result_csv = roundtrip_test_with_args(
        "csv",
        "parquet",
        SAMPLE_CSV_DATA,
        &["--include-columns", "name,city"],
    );
    assert_csv_contains_lines(
        &result_csv,
        &["name,city", "Alice,NYC", "Bob,SF"],
    );
}

#[test]
fn test_csv_to_avro_roundtrip() {
    let result_csv = roundtrip_test_with_args("csv", "avro", SAMPLE_CSV_DATA, &[]);
    assert_csv_contains_lines(
        &result_csv,
        &["name,age,city", "Alice,30,NYC", "Bob,25,SF"],
    );
}

#[test]
fn test_csv_to_ipc_roundtrip() {
    let result_csv = roundtrip_test_with_args("csv", "ipc", SAMPLE_CSV_DATA, &[]);
    assert_csv_contains_lines(
        &result_csv,
        &["name,age,city", "Alice,30,NYC", "Bob,25,SF"],
    );
}

#[test]
fn test_csv_to_json_roundtrip() {
    let result_csv = roundtrip_test_with_args("csv", "json", SAMPLE_CSV_DATA, &[]);
    assert_csv_contains_lines(
        &result_csv,
        &["name,age,city", "Alice,30,NYC", "Bob,25,SF"],
    );
}

#[test]
fn test_csv_to_csv_row_filter_eq_age() {
    // We'll spool an input CSV with two rows, ages 25 and 40
    let csv_data = "\
name,age,city
Alice,25,NYC
Charlie,40,LA
";

    let tmp = tempdir().expect("Unable to create temp dir");
    let input_csv_path = tmp.path().join("input.csv");
    let output_csv_path = tmp.path().join("output.csv");
    fs::write(&input_csv_path, csv_data).expect("Unable to write test CSV");

    // Filter for "age == 25"
    Command::cargo_bin("frameblaze")
        .unwrap()
        .args([
            "csv",
            "csv",
            input_csv_path.to_str().unwrap(),
            "--output",
            output_csv_path.to_str().unwrap(),
            "--row-filter-col",
            "age",
            "--row-filter-op",
            "eq",
            "--row-filter-val",
            "25",
        ])
        .assert()
        .success();

    // read result
    let out_data = fs::read_to_string(&output_csv_path).unwrap();
    // only "Alice,25,NYC"
    assert!(out_data.contains("Alice,25,NYC"));
    assert!(!out_data.contains("Charlie,40,LA"));
}

#[test]
fn test_csv_to_csv_row_filter_gt_age() {
    let csv_data = "\
name,age,city
Alice,25,NYC
Bob,30,SF
Charlie,35,CHI
";

    let tmp = tempdir().expect("Unable to create temp dir");
    let input_csv_path = tmp.path().join("input.csv");
    let output_csv_path = tmp.path().join("output.csv");
    fs::write(&input_csv_path, csv_data).expect("Unable to write test CSV");

    // Filter for "age > 25"
    Command::cargo_bin("frameblaze")
        .unwrap()
        .args([
            "csv",
            "csv",
            input_csv_path.to_str().unwrap(),
            "--output",
            output_csv_path.to_str().unwrap(),
            "--row-filter-col",
            "age",
            "--row-filter-op",
            "gt",
            "--row-filter-val",
            "25",
        ])
        .assert()
        .success();

    let out_data = fs::read_to_string(&output_csv_path).unwrap();
    // should contain Bob(30) and Charlie(35), but not Alice(25)
    assert!(!out_data.contains("Alice,25,NYC"));
    assert!(out_data.contains("Bob,30,SF"));
    assert!(out_data.contains("Charlie,35,CHI"));
}
