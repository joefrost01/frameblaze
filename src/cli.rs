use crate::format::Format;
use clap::{ArgAction, Parser};

#[derive(Parser, Debug)]
#[command(
    name = "frameblaze",
    version = "0.1.0",
    about = "Convert between CSV/Parquet/Avro/IPC/JSON with optional column and row filtering."
)]
pub struct Cli {
    /// Source format (csv, parquet, avro, ipc, json)
    #[arg(value_enum)]
    pub from_format: Format,

    /// Target format (csv, parquet, avro, ipc, json)
    #[arg(value_enum)]
    pub to_format: Format,

    /// Path to the input file
    pub input_file: String,

    /// Path to the output file
    #[arg(long, short = 'o', action = ArgAction::Set)]
    pub output: Option<String>,

    /// Append to existing output (CSV only)
    #[arg(long, action = ArgAction::SetTrue)]
    pub append: bool,

    /// Comma-separated list of columns to keep
    #[arg(long = "include-columns", value_delimiter = ',', required = false)]
    pub include_columns: Option<Vec<String>>,

    /// Comma-separated list of columns to drop
    #[arg(long = "exclude-columns", value_delimiter = ',', required = false)]
    pub exclude_columns: Option<Vec<String>>,

    /// The column on which to filter rows (e.g. "age")
    #[arg(long = "row-filter-col", required = false)]
    pub row_filter_col: Option<String>,

    /// The operator to use for row filtering (eq, gt, lt)
    #[arg(long = "row-filter-op", required = false)]
    pub row_filter_op: Option<String>,

    /// The value to compare against (e.g. "25" or "LA")
    #[arg(long = "row-filter-val", required = false)]
    pub row_filter_val: Option<String>,
}
