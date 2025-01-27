use anyhow::Result;
use polars::prelude::*;

pub trait ToFile {
    fn write_data(&self, path: &str, df: &DataFrame, append: bool) -> Result<()>;
}

// The `DataWriter` enum acts as a dispatcher for multiple writer implementations
pub enum DataWriter {
    Csv(csv::CsvWriterImpl),
    Parquet(parquet::ParquetWriterImpl),
    Avro(avro::AvroWriterImpl),
    Ipc(ipc::IpcWriterImpl),
    Json(json::JsonWriterImpl),
}

impl DataWriter {
    pub fn write_data(&self, path: &str, df: &DataFrame, append: bool) -> Result<()> {
        match self {
            DataWriter::Csv(w) => w.write_data(path, df, append),
            DataWriter::Parquet(w) => w.write_data(path, df, append),
            DataWriter::Avro(w) => w.write_data(path, df, append),
            DataWriter::Ipc(w) => w.write_data(path, df, append),
            DataWriter::Json(w) => w.write_data(path, df, append),
        }
    }
}

pub mod csv;
pub mod parquet;
pub mod avro;
pub mod ipc;
pub mod json;