use anyhow::Result;
use polars::prelude::*;

pub trait FromFile {
    fn read_data(&self, path: &str) -> Result<DataFrame>;
}

// The `DataReader` enum acts as a dispatcher for multiple reader implementations
pub enum DataReader {
    Csv(csv::CsvReaderImpl),
    Parquet(parquet::ParquetReaderImpl),
}

impl DataReader {
    pub fn read_data(&self, path: &str) -> Result<DataFrame> {
        match self {
            DataReader::Csv(r) => r.read_data(path),
            DataReader::Parquet(r) => r.read_data(path),
        }
    }
}

pub mod csv;
pub mod parquet;