use anyhow::Result;
use polars::prelude::*;

pub trait FromFile {
    fn read_data(&self, path: &str) -> Result<DataFrame>;
}

// The `DataReader` enum acts as a dispatcher for multiple reader implementations
pub enum DataReader {
    Csv(csv::CsvReaderImpl),
    Parquet(parquet::ParquetReaderImpl),
    Avro(avro::AvroReaderImpl),
    Ipc(ipc::IpcReaderImpl),
    Json(json::JsonReaderImpl),
}

impl DataReader {
    pub fn read_data(&self, path: &str) -> Result<DataFrame> {
        match self {
            DataReader::Csv(r) => r.read_data(path),
            DataReader::Parquet(r) => r.read_data(path),
            DataReader::Avro(r) => r.read_data(path),
            DataReader::Ipc(r) => r.read_data(path),
            DataReader::Json(r) => r.read_data(path),
        }
    }
}

pub mod csv;
pub mod parquet;
pub mod avro;
pub mod ipc;
pub mod json;