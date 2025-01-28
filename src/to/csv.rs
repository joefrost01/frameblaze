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

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::helpers::*;
    use std::error::Error;

    fn read_fn(path: &str) -> Result<DataFrame, Box<dyn Error>> {
        let file = File::open(path)?;
        let df = CsvReader::new(file).finish()?;
        Ok(df)
    }

    fn writer() -> CsvWriterImpl {
        CsvWriterImpl::default()
    }

    #[test]
    fn test_write_valid_file() -> Result<()> {
        test_write_read_compare(&writer(), read_fn, false)?;
        Ok(())
    }

    #[test]
    fn test_write_to_nonexistent_directory() -> Result<()> {
        test_write_should_fail(&writer());
        Ok(())
    }

    #[test]
    fn test_write_overwrite_file() -> Result<()> {
        test_write_overwrite(&writer(), read_fn)?;
        Ok(())
    }
}
