use anyhow::Result;
use polars::prelude::*;
use std::fs::File;
use polars_io::avro::AvroReader;

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
