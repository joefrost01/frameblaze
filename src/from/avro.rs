use anyhow::Result;
use polars::prelude::*;
use std::fs::File;
use polars_io::avro::AvroReader;

#[derive(Default)]
pub struct AvroReaderImpl;

impl super::FromFile for AvroReaderImpl {
    fn read_data(&self, path: &str) -> Result<DataFrame> {
        let file = File::open(path)?;
        let df = AvroReader::new(file)
            .finish()?;
        Ok(df)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use polars_io::avro::AvroWriter;
    use tempfile::NamedTempFile;
    use crate::from::FromFile;

    #[test]
    fn test_read_valid_avro() {
        // Create a DataFrame and write it to a temporary Parquet file
        let mut temp_file = NamedTempFile::new().unwrap();
        let df = df! {
        "name" => &["Alice", "Bob"],
        "age" => &[30, 25],
        "city" => &["New York", "Los Angeles"]
    }.unwrap();

        // Pass `temp_file` directly to ParquetWriter
        AvroWriter::new(&mut temp_file)
            .finish(&mut df.clone())
            .unwrap();

        // Instantiate ParquetReaderImpl and read the file
        let reader = AvroReaderImpl::default();
        let result = reader.read_data(temp_file.path().to_str().unwrap());

        // Verify the DataFrame
        let df_read = result.unwrap();
        assert!(df.equals(&df_read));
        assert_eq!(df_read.shape(), (2, 3)); // 2 rows, 3 columns
        assert_eq!(df_read.get_column_names(), &["name", "age", "city"]);
    }

    #[test]
    fn test_missing_avro_file() {
        // Attempt to read a non-existent file
        let reader = AvroReaderImpl::default();
        let result = reader.read_data("non_existent_file.parquet");

        // Verify that an error is returned
        assert!(result.is_err());
    }

    #[test]
    fn test_read_malformed_avro() {
        // Create a malformed Parquet file
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "this is not valid parquet data").unwrap();

        // Instantiate ParquetReaderImpl and attempt to read the file
        let reader = AvroReaderImpl::default();
        let result = reader.read_data(temp_file.path().to_str().unwrap());

        // Verify that an error is returned
        assert!(result.is_err());
    }
}
