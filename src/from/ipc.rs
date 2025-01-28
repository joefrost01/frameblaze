use anyhow::Result;
use polars::prelude::*;
use std::fs::File;

#[derive(Default)]
pub struct IpcReaderImpl;

impl super::FromFile for IpcReaderImpl {
    fn read_data(&self, path: &str) -> Result<DataFrame> {
        let file = File::open(path)?;
        let df = IpcReader::new(file)
            .finish()?;
        Ok(df)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;
    use crate::from::FromFile;

    #[test]
    fn test_read_valid_arrow() {
        // Create a DataFrame and write it to a temporary Parquet file
        let mut temp_file = NamedTempFile::new().unwrap();
        let df = df! {
        "name" => &["Alice", "Bob"],
        "age" => &[30, 25],
        "city" => &["New York", "Los Angeles"]
    }
            .unwrap();

        // Pass `temp_file` directly to ParquetWriter
        IpcWriter::new(&mut temp_file)
            .finish(&mut df.clone())
            .unwrap();

        // Instantiate ParquetReaderImpl and read the file
        let reader = IpcReaderImpl::default();
        let result = reader.read_data(temp_file.path().to_str().unwrap());

        // Verify the DataFrame
        let df_read = result.unwrap();
        assert_eq!(df_read.shape(), (2, 3)); // 2 rows, 3 columns
        assert_eq!(df_read.get_column_names(), &["name", "age", "city"]);
    }

    #[test]
    fn test_missing_arrow_file() {
        // Attempt to read a non-existent file
        let reader = IpcReaderImpl::default();
        let result = reader.read_data("non_existent_file.parquet");

        // Verify that an error is returned
        assert!(result.is_err());
    }

    #[test]
    fn test_read_malformed_arrow() {
        // Create a malformed Parquet file
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "this is not valid parquet data").unwrap();

        // Instantiate ParquetReaderImpl and attempt to read the file
        let reader = IpcReaderImpl::default();
        let result = reader.read_data(temp_file.path().to_str().unwrap());

        // Verify that an error is returned
        assert!(result.is_err());
    }
}