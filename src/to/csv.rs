use anyhow::Result;
use polars::prelude::*;
use std::fs::File;

#[derive(Default)]
pub struct CsvWriterImpl;

impl super::ToFile for CsvWriterImpl {
    fn write_data(&self, path: &str, df: &DataFrame, _append: bool) -> Result<()> {

        let file = File::create(path)?;

        let mut df_to_write = df.clone();

        CsvWriter::new(file)
            .finish(&mut df_to_write)?;

        // 4. Done!
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use std::fs;
    use crate::to::ToFile;

    #[test]
    fn test_write_valid_csv() {
        // Create a DataFrame
        let df = df! {
            "name" => &["Alice", "Bob"],
            "age" => &[30, 25],
            "city" => &["New York", "Los Angeles"]
        }
            .unwrap();

        // Create a temporary file
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_str().unwrap();

        // Write the DataFrame to the file
        let writer = CsvWriterImpl::default();
        writer.write_data(file_path, &df, false).unwrap();

        // Read back the file contents
        let contents = fs::read_to_string(file_path).unwrap();
        let expected = "name,age,city\nAlice,30,New York\nBob,25,Los Angeles\n";

        assert_eq!(contents, expected);
    }

    #[test]
    fn test_write_to_nonexistent_directory() {
        // Create a DataFrame
        let df = df! {
            "name" => &["Alice"],
            "age" => &[30],
            "city" => &["New York"]
        }
            .unwrap();

        // Try writing to a non-existent directory
        let file_path = "/nonexistent_dir/output.csv";

        // Write the DataFrame
        let writer = CsvWriterImpl::default();
        let result = writer.write_data(file_path, &df, false);

        // Ensure an error is returned
        assert!(result.is_err());
    }
}
