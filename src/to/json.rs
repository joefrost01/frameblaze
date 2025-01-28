use anyhow::Result;
use polars::prelude::*;
use std::fs::File;

#[derive(Default)]
pub struct JsonWriterImpl;

impl super::ToFile for JsonWriterImpl {
    fn write_data(&self, path: &str, df: &DataFrame, _append: bool) -> Result<()> {
        // 1) Create or overwrite the file
        let file = File::create(path)?;

        // 2) Use JsonWriter (NDJSON by default)
        let mut writer = JsonWriter::new(file)
            .with_json_format(JsonFormat::JsonLines); // line-delimited

        // 3) Polars requires a mutable reference to the DataFrame
        writer.finish(&mut df.clone())?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::to::ToFile;
    use polars::io::json::JsonReader;
    use tempfile::NamedTempFile;
    use std::fs::File;

    #[test]
    fn test_write_valid_json() {
        // Create a temporary file for the JSON output
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_str().unwrap();

        // Create a sample DataFrame
        let df = df! {
            "name" => &["Alice", "Bob", "Charlie"],
            "age" => &[30, 25, 35],
            "city" => &["New York", "Los Angeles", "Chicago"]
        }
            .unwrap();

        // Write the DataFrame to JSON
        let writer = JsonWriterImpl::default();
        writer.write_data(file_path, &df, false).unwrap();

        // Read the JSON file back as NDJSON
        let read_df = JsonReader::new(File::open(file_path).unwrap())
            // If needed, specify the format:
            // .with_json_format(JsonFormat::JsonLines) // line-delimited
            .with_json_format(JsonFormat::JsonLines)
            .finish()
            .unwrap();

        // Ensure the written and read DataFrames match
        assert_eq!(df.shape(), read_df.shape());
        assert_eq!(df.get_column_names(), read_df.get_column_names());
        assert_eq!(df.get_row(0).unwrap(), read_df.get_row(0).unwrap());
    }

    #[test]
    fn test_write_json_to_nonexistent_directory() {
        // Define a path in a nonexistent directory
        let file_path = "/nonexistent_dir/test.json";

        // Create a sample DataFrame
        let df = df! {
            "name" => &["Alice", "Bob"],
            "age" => &[30, 25]
        }
            .unwrap();

        // Attempt to write the DataFrame to the nonexistent path
        let writer = JsonWriterImpl::default();
        let result = writer.write_data(file_path, &df, false);

        // Verify that an error occurs
        assert!(result.is_err());
    }

    #[test]
    fn test_write_overwrite_json_file() {
        // Create a temporary file for the JSON output
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_str().unwrap();

        // Create an initial DataFrame
        let df1 = df! {
            "name" => &["Alice", "Bob"],
            "age" => &[30, 25]
        }
            .unwrap();

        // Write the first DataFrame
        let writer = JsonWriterImpl::default();
        writer.write_data(file_path, &df1, false).unwrap();

        // Create a second DataFrame with different data
        let df2 = df! {
            "name" => &["Charlie", "Tracy"],
            "age" => &[35, 28]
        }
            .unwrap();

        // Overwrite the existing file with the new DataFrame
        writer.write_data(file_path, &df2, false).unwrap();

        // Read the JSON file back
        let read_df = JsonReader::new(File::open(file_path).unwrap())
            .with_json_format(JsonFormat::JsonLines)
            .finish()
            .unwrap();

        // Ensure the file contains the second DataFrame
        assert_eq!(df2.shape(), read_df.shape());
        assert_eq!(df2.get_column_names(), read_df.get_column_names());
        assert_eq!(df2.get_row(0).unwrap(), read_df.get_row(0).unwrap());
    }
}
