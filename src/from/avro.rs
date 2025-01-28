use anyhow::Result;
use polars::prelude::*;
use polars_io::avro::AvroReader;
use std::fs::File;

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
    use crate::from::FromFile;
    use crate::test_utils::helpers::*;
    use polars_io::avro::AvroWriter;
    use tempfile::NamedTempFile;

    fn reader() -> AvroReaderImpl {
        AvroReaderImpl::default()
    }

    #[test]
    fn test_read_valid_df() {
        let writer_fn = |temp_file: &mut NamedTempFile, df: &DataFrame| -> Result<()> {
            AvroWriter::new(temp_file).finish(&mut df.clone())?;
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
    fn test_read_malformed_avro() {
        let temp_file = create_malformed_file();
        let result = reader().read_data(temp_file.unwrap().path().to_str().unwrap());
        assert!(result.is_err());
    }
}
