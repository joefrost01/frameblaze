use anyhow::Result;
use polars::prelude::*;
use std::fs::File;

#[derive(Default)]
pub struct JsonWriterImpl;

impl super::ToFile for JsonWriterImpl {
    fn write_data(&self, path: &str, df: &DataFrame, _append: bool) -> Result<()> {
        let file = File::create(path)?;

        let mut writer = JsonWriter::new(file)
            .with_json_format(JsonFormat::JsonLines); // line-delimited

        writer.finish(&mut df.clone())?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::helpers::*;
    use polars::io::json::JsonReader;
    use std::error::Error;
    use std::fs::File;

    fn read_fn(path: &str) -> Result<DataFrame, Box<dyn Error>> {
        let file = File::open(path)?;
        let df = JsonReader::new(file).with_json_format(JsonFormat::JsonLines).finish()?;
        Ok(df)
    }

    fn writer() -> JsonWriterImpl {
        JsonWriterImpl::default()
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
