use crate::storage::Storage;
use anyhow::Result;
use polars::prelude::*;

#[derive(Default)]
pub struct IpcReaderImpl;

impl super::FromFile for IpcReaderImpl {
    fn read_data(&self, path: &str) -> Result<DataFrame> {
        let mut storage = Storage::new(path)?;
        let file = storage.get_source_file()?;
        let df = IpcReader::new(file)
            .finish()?;
        Ok(df)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::from::FromFile;
    use crate::test_utils::helpers::*;
    use tempfile::NamedTempFile;

    fn reader() -> IpcReaderImpl {
        IpcReaderImpl::default()
    }

    #[test]
    fn test_read_valid_df() {
        let writer_fn = |temp_file: &mut NamedTempFile, df: &DataFrame| -> Result<()> {
            IpcWriter::new(temp_file).finish(&mut df.clone())?;
            Ok(())
        };
        let reader = Box::new(reader());
        test_write_then_read(writer_fn, reader).unwrap();
    }

    #[test]
    fn test_missing_source_file() {
        let result = reader().read_data("non_existent_file");
        assert!(result.is_err());
    }

    #[test]
    fn test_read_malformed_file() {
        let temp_file = create_malformed_file();
        let result = reader().read_data(temp_file.unwrap().path().to_str().unwrap());
        assert!(result.is_err());
    }
}