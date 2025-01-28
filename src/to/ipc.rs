use crate::storage::Storage;
use anyhow::Result;
use polars::prelude::*;
use std::fs::File;

#[derive(Default)]
pub struct IpcWriterImpl;

impl super::ToFile for IpcWriterImpl {
    fn write_data(&self, path: &str, df: &DataFrame, _append: bool) -> Result<()> {
        let mut storage = Storage::new(path)?;
        let file: File = storage.get_target_file()?;
        let mut df_to_write = df.clone();
        IpcWriter::new(file).finish(&mut df_to_write)?;
        storage.finish_write()?;
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
        let df = IpcReader::new(file).finish()?;
        Ok(df)
    }

    fn writer() -> IpcWriterImpl {
        IpcWriterImpl::default()
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

    #[test]
    fn test_write_empty() -> Result<()> {
        test_write_empty_dataframe(&writer(), read_fn)?;
        Ok(())
    }
}