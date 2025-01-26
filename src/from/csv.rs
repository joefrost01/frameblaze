use anyhow::Result;
use polars::prelude::*;
use std::fs::File;

#[derive(Default)]
pub struct CsvReaderImpl;

impl super::FromFile for CsvReaderImpl {
    fn read_data(&self, path: &str) -> Result<DataFrame> {
        let file = File::open(path)?;
        let df = CsvReader::new(file)
            .finish()?;
        Ok(df)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::from::FromFile;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_read_valid_csv() {
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "name,age,city").unwrap();
        writeln!(temp_file, "Alice,30,New York").unwrap();
        writeln!(temp_file, "Bob,25,Los Angeles").unwrap();

        let reader = CsvReaderImpl::default();
        let df = reader.read_data(temp_file.path().to_str().unwrap()).unwrap();

        assert_eq!(df.shape(), (2, 3));
        assert_eq!(df.get_column_names(), &["name", "age", "city"]);
    }

    #[test]
    fn test_read_empty_csv() {
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "name,age,city").unwrap();

        let reader = CsvReaderImpl::default();
        let df = reader.read_data(temp_file.path().to_str().unwrap()).unwrap();

        assert_eq!(df.shape(), (0, 3));
        assert_eq!(df.get_column_names(), &["name", "age", "city"]);
    }

    #[test]
    fn test_missing_file() {
        let reader = CsvReaderImpl::default();
        let result = reader.read_data("non_existent_file.csv");
        assert!(result.is_err());
    }

}
