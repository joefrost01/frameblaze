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
            .finish()?;
        Ok(df)
    }
}