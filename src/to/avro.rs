use anyhow::Result;
use polars::prelude::*;
use std::fs::File;
use polars_io::avro::AvroWriter;

#[derive(Default)]
pub struct AvroWriterImpl;

impl super::ToFile for AvroWriterImpl {
    fn write_data(&self, path: &str, df: &DataFrame, _append: bool) -> Result<()> {

        let file = File::create(path)?;

        let mut df_to_write = df.clone();

        AvroWriter::new(file).finish(&mut df_to_write)?;

        Ok(())
    }
}
