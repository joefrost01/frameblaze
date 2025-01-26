use clap::{Arg, ArgAction, Command};
use anyhow::Result;

mod config;
mod r#from;
mod r#to;
mod transform;

use config::Config;
use r#from::{DataReader, csv::CsvReaderImpl, parquet::ParquetReaderImpl};
use r#to::{DataWriter, csv::CsvWriterImpl, parquet::ParquetWriterImpl};
use transform::{Transform, column_filter::ColumnFilter};

fn main() -> Result<()> {
    let matches = build_cli().get_matches();

    // Retrieve positional arguments as strings
    let from_format: &String = matches.get_one("from_format").unwrap();
    let to_format: &String = matches.get_one("to_format").unwrap();
    let input_file: &String = matches.get_one("input_file").unwrap();

    // Retrieve `--output` as an optional String
    let output_file: Option<&String> = matches.get_one("output");

    // Retrieve optional column filters
    let include_str: Option<&String> = matches.get_one("include_columns");
    let exclude_str: Option<&String> = matches.get_one("exclude_columns");

    // Retrieve boolean for `--append`
    let append = matches.get_flag("append");

    let config = Config::new(
        from_format.clone(),
        to_format.clone(),
        input_file.clone(),
        output_file.cloned(),
        // Convert the &String to a String, then split on commas
        include_str.map(|s| s.split(',').map(|x| x.trim().to_string()).collect()),
        exclude_str.map(|s| s.split(',').map(|x| x.trim().to_string()).collect()),
    );
    config.validate()?;

    // Construct readers/writers based on config
    let reader = match config.from_format.as_str() {
        "csv" => DataReader::Csv(CsvReaderImpl::default()),
        "parquet" => DataReader::Parquet(ParquetReaderImpl::default()),
        _ => panic!("Unsupported from_format"),
    };

    let writer = match config.to_format.as_str() {
        "csv" => DataWriter::Csv(CsvWriterImpl::default()),
        "parquet" => DataWriter::Parquet(ParquetWriterImpl::default()),
        _ => panic!("Unsupported to_format"),
    };

    // Read entire DataFrame - TODO implement chunking to allow dfs larger than memory
    let df = reader.read_data(&config.input_file)?;

    // Apply column filter transformation
    let column_filter = ColumnFilter::new(config.include_columns.clone(),
                                          config.exclude_columns.clone());
    let df_transformed = column_filter.transform(df)?;

    // Write to output (append only relevant to CSV for now)
    writer.write_data(config.output_file.as_ref().unwrap(), &df_transformed, append)?;

    Ok(())
}

fn build_cli() -> Command {
    Command::new("frameblaze")
        .version("0.1.0")
        .about("A minimal MVP for converting Parquet <-> CSV with column filtering.")
        // Three positional args: from_format, to_format, input_file
        .arg(
            Arg::new("from_format")
                .help("Source format: 'csv' or 'parquet'")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::new("to_format")
                .help("Target format: 'csv' or 'parquet'")
                .required(true)
                .index(2),
        )
        .arg(
            Arg::new("input_file")
                .help("Path to input file")
                .required(true)
                .index(3),
        )
        // Optional flags/parameters
        .arg(
            Arg::new("output")
                .long("output")
                .help("Output file path")
                .action(ArgAction::Set), // sets a String
        )
        .arg(
            Arg::new("append")
                .long("append")
                .help("Append to existing output (CSV only)")
                .action(ArgAction::SetTrue), // sets a bool
        )
        .arg(
            Arg::new("include_columns")
                .long("include-columns")
                .help("Comma-separated list of columns to keep")
                .action(ArgAction::Set), // sets a String
        )
        .arg(
            Arg::new("exclude_columns")
                .long("exclude-columns")
                .help("Comma-separated list of columns to drop")
                .action(ArgAction::Set),
        )
}
