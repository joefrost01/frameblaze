use anyhow::Result;
use clap::Parser;

mod cli;
mod config;
mod format;  // Where the Format enum is
mod r#from;
mod r#to;
mod transform;
mod test_utils;

use crate::cli::Cli;
use crate::config::Config;
use crate::format::Format;
use crate::from::{avro::AvroReaderImpl, ipc::IpcReaderImpl, json::JsonReaderImpl};
use crate::to::{avro::AvroWriterImpl, ipc::IpcWriterImpl, json::JsonWriterImpl};
use r#from::{csv::CsvReaderImpl, parquet::ParquetReaderImpl, DataReader};
use r#to::{csv::CsvWriterImpl, parquet::ParquetWriterImpl, DataWriter};
use transform::{column_filter::ColumnFilter, Transform};

fn main() -> Result<()> {
    // 1. Parse CLI
    let cli = Cli::parse();

    // 2. Build config from CLI
    let config = Config::new(
        cli.from_format,
        cli.to_format,
        cli.input_file,
        cli.output,
        cli.include_columns,
        cli.exclude_columns,
    );
    config.validate()?;

    // 3. Create reader based on enum
    let reader = match config.from_format {
        Format::Csv => DataReader::Csv(CsvReaderImpl::default()),
        Format::Parquet => DataReader::Parquet(ParquetReaderImpl::default()),
        Format::Avro => DataReader::Avro(AvroReaderImpl::default()),
        Format::Ipc => DataReader::Ipc(IpcReaderImpl::default()),
        Format::Json => DataReader::Json(JsonReaderImpl::default()),
    };

    // 4. Create writer based on enum
    let writer = match config.to_format {
        Format::Csv => DataWriter::Csv(CsvWriterImpl::default()),
        Format::Parquet => DataWriter::Parquet(ParquetWriterImpl::default()),
        Format::Avro => DataWriter::Avro(AvroWriterImpl::default()),
        Format::Ipc => DataWriter::Ipc(IpcWriterImpl::default()),
        Format::Json => DataWriter::Json(JsonWriterImpl::default()),
    };

    // 5. Read DataFrame
    let df = reader.read_data(&config.input_file)?;

    // 6. Apply transformations (column filtering)
    let column_filter = ColumnFilter::new(
        config.include_columns.clone(),
        config.exclude_columns.clone(),
    );
    let df_transformed = column_filter.transform(df)?;

    // 7. Write DataFrame
    writer.write_data(
        config
            .output_file
            .as_ref()
            .expect("Output file must be provided via --output"),
        &df_transformed,
        cli.append,
    )?;

    Ok(())
}