use anyhow::Result;
use polars::prelude::*;
use std::fs::File;

#[derive(Default)]
pub struct JsonReaderImpl;

impl super::FromFile for JsonReaderImpl {
    fn read_data(&self, path: &str) -> Result<DataFrame> {
        let file = File::open(path)?;
        // By default, this interprets the file as NDJSON (one JSON object per line)
        let df = JsonReader::new(file)
            .with_json_format(JsonFormat::JsonLines)
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

    fn reader() -> JsonReaderImpl {
        JsonReaderImpl::default()
    }

    #[test]
    fn test_read_valid_df() {
        let writer_fn = |temp_file: &mut NamedTempFile, df: &DataFrame| -> Result<()> {
            JsonWriter::new(temp_file).with_json_format(JsonFormat::JsonLines).finish(&mut df.clone())?;
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