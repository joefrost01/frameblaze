use clap::ValueEnum;

#[derive(Clone, Debug, Copy, PartialEq, Eq, ValueEnum)]
pub enum Format {
    Csv,
    Parquet,
    Avro,
    Ipc,
    Json,
}