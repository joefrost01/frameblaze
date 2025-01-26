// tests/integration_test.rs

use assert_cmd::Command;
use std::fs;
use tempfile::tempdir;

#[test]
fn test_csv_to_parquet_roundtrip() {
    let tmp = tempdir().unwrap();
    let input_csv_path = tmp.path().join("input.csv");
    let intermediate_parquet_path = tmp.path().join("intermediate.parquet");
    let output_csv_path = tmp.path().join("output.csv");

    // Write a small CSV
    let csv_data = "\
name,age,city
Alice,30,NYC
Bob,25,SF
";
    fs::write(&input_csv_path, csv_data).expect("Unable to write test CSV");

    // 1. Convert CSV -> Parquet
    Command::cargo_bin("frameblaze")
        .unwrap()
        .args([
            "csv",
            "parquet",
            input_csv_path.to_str().unwrap(),
            "--output",
            intermediate_parquet_path.to_str().unwrap(),
        ])
        .assert()
        .success();

    // 2. Convert Parquet -> CSV (exclude 'city' column)
    Command::cargo_bin("frameblaze")
        .unwrap()
        .args([
            "parquet",
            "csv",
            intermediate_parquet_path.to_str().unwrap(),
            "--output",
            output_csv_path.to_str().unwrap(),
            "--exclude-columns",
            "city",
        ])
        .assert()
        .success();

    // Check output CSV
    let result_csv = fs::read_to_string(&output_csv_path).expect("Unable to read output CSV");
    // Should only have columns name, age
    assert!(result_csv.contains("name,age"));
    assert!(!result_csv.contains("city"));
    assert!(result_csv.contains("Alice,30"));
    assert!(result_csv.contains("Bob,25"));
}
