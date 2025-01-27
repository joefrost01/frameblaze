use anyhow::Result;
use polars::prelude::*;
use polars::io::json::JsonWriter;
use std::fs::File;

#[derive(Default)]
pub struct JsonWriterImpl;

impl super::ToFile for JsonWriterImpl {
    fn write_data(&self, path: &str, df: &DataFrame, _append: bool) -> Result<()> {
        // 1) Create or overwrite the file.
        let file = File::create(path)?;

        // 2) (Optional) If you want to produce a single JSON array instead of NDJSON lines:
        // let mut writer = JsonWriter::new(file)
        //     .with_json_format(JsonFormat::Json); // single array

        // NDJSON by default:
        let mut writer = JsonWriter::new(file);

        // 3) Polars requires a mutable reference to the DataFrame.
        //    This writes the DataFrame in the chosen JSON format.
        writer.finish(&mut df.clone())?;

        Ok(())
    }
}
