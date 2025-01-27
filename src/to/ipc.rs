use anyhow::Result;
use polars::prelude::*;
use std::fs::File;

#[derive(Default)]
pub struct IpcWriterImpl;

impl super::ToFile for IpcWriterImpl {
    fn write_data(&self, path: &str, df: &DataFrame, _append: bool) -> Result<()> {
        let file = File::create(path)?;

        let mut df_to_write = df.clone();

        IpcWriter::new(file).finish(&mut df_to_write)?;

        Ok(())
    }
}
