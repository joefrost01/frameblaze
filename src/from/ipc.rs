use anyhow::Result;
use polars::prelude::*;
use std::fs::File;

#[derive(Default)]
pub struct IpcReaderImpl;

impl super::FromFile for IpcReaderImpl {
    fn read_data(&self, path: &str) -> Result<DataFrame> {
        let file = File::open(path)?;
        let df = IpcReader::new(file)
            .finish()?;
        Ok(df)
    }
}
