use anyhow::Result;
use clap::Parser;

mod cli;
mod config;
mod format;  // Where the Format enum is
mod r#from;
mod r#to;
mod transform;
mod test_utils;
mod storage;

use crate::cli::Cli;
use crate::config::Config;
use crate::format::Format;
use crate::from::{avro::AvroReaderImpl, ipc::IpcReaderImpl, json::JsonReaderImpl};
use crate::to::{avro::AvroWriterImpl, ipc::IpcWriterImpl, json::JsonWriterImpl};
use r#from::{csv::CsvReaderImpl, parquet::ParquetReaderImpl, DataReader};
use r#to::{csv::CsvWriterImpl, parquet::ParquetWriterImpl, DataWriter};
use transform::{column_filter::ColumnFilter, row_filter::{RowFilter, RowFilterValue, RowFilterOp}, Transform};

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
        cli.row_filter_col,
        cli.row_filter_op,
        cli.row_filter_val,
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

    // 6. Column Filtering
    let column_filter = ColumnFilter::new(
        config.include_columns.clone(),
        config.exclude_columns.clone(),
    );
    let mut df_transformed = column_filter.transform(df)?;

    // 7. Check if we have row-filter arguments
    if let (Some(col), Some(op_str), Some(val)) =
        (&config.row_filter_col, &config.row_filter_op, &config.row_filter_val)
    {
        // parse operator into an Option<RowFilterOp>
        let op = match op_str.as_str() {
            "eq" => Some(RowFilterOp::Eq),
            "gt" => Some(RowFilterOp::Gt),
            "lt" => Some(RowFilterOp::Lt),
            _ => {
                eprintln!("Invalid row filter operator: {}. Ignoring row filter.", op_str);
                None
            }
        };

        if let Some(parsed_op) = op {
            // parse val as i64 or treat as string
            let val_as_int = val.parse::<i64>();
            let row_value = if let Ok(i) = val_as_int {
                RowFilterValue::Int(i)
            } else {
                RowFilterValue::Str(val.to_string())
            };

            let row_filter = RowFilter::new(col, parsed_op, row_value);
            df_transformed = row_filter.transform(df_transformed)?;
        }
    }


    // 8. Write DataFrame
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
