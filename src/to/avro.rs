use anyhow::Result;
use polars::prelude::*;
use std::fs::File;
use polars_io::avro::AvroWriter;

#[derive(Default)]
pub struct AvroWriterImpl;

impl super::ToFile for AvroWriterImpl {
    fn write_data(&self, path: &str, df: &DataFrame, _append: bool) -> Result<()> {

        let file = File::create(path)?;

        let mut df_to_write = df.clone();

        AvroWriter::new(file).finish(&mut df_to_write)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use polars_io::avro::AvroReader;
    use super::*;
    use crate::to::ToFile;
    use tempfile::NamedTempFile;

    #[test]
    fn test_write_valid_avro() {
        // Create a temporary file for the Parquet output
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_str().unwrap();

        // Create a sample DataFrame
        let df = df! {
            "name" => &["Alice", "Bob", "Charlie"],
            "age" => &[30, 25, 35],
            "city" => &["New York", "Los Angeles", "Chicago"]
        }
            .unwrap();

        // Write the DataFrame to Parquet
        let writer = AvroWriterImpl::default();
        writer.write_data(file_path, &df, false).unwrap();

        // Read the Parquet file back
        let read_df = AvroReader::new(File::open(file_path).unwrap())
            .finish()
            .unwrap();

        // Ensure the written and read DataFrames match
        assert_eq!(df.shape(), read_df.shape());
        assert_eq!(df.get_column_names(), read_df.get_column_names());
        assert_eq!(df.get_row(0).unwrap(), read_df.get_row(0).unwrap());
    }

    #[test]
    fn test_write_avro_to_nonexistent_directory() {
        // Define a path in a nonexistent directory
        let file_path = "/nonexistent_dir/test.parquet";

        // Create a sample DataFrame
        let df = df! {
            "name" => &["Alice", "Bob"],
            "age" => &[30, 25]
        }
            .unwrap();

        // Attempt to write the DataFrame to the nonexistent path
        let writer = AvroWriterImpl::default();
        let result = writer.write_data(file_path, &df, false);

        // Verify that an error occurs
        assert!(result.is_err());
    }

    #[test]
    fn test_write_overwrite_avro_file() {
        // Create a temporary file for the Parquet output
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_str().unwrap();

        // Create an initial DataFrame
        let df1 = df! {
            "name" => &["Alice", "Bob"],
            "age" => &[30, 25]
        }
            .unwrap();

        // Write the first DataFrame to Parquet
        let writer = AvroWriterImpl::default();
        writer.write_data(file_path, &df1, false).unwrap();

        // Create a second DataFrame with different data
        let df2 = df! {
            "name" => &["Charlie", "Tracy"],
            "age" => &[35, 28]
        }
            .unwrap();

        // Overwrite the existing Parquet file with the new DataFrame
        writer.write_data(file_path, &df2, false).unwrap();

        // Read the Parquet file back
        let read_df = AvroReader::new(File::open(file_path).unwrap())
            .finish()
            .unwrap();

        // Ensure the file contains the second DataFrame
        assert_eq!(df2.shape(), read_df.shape());
        assert_eq!(df2.get_column_names(), read_df.get_column_names());
        assert_eq!(df2.get_row(0).unwrap(), read_df.get_row(0).unwrap());
    }
}