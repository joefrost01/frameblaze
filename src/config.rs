use crate::format::Format;
use anyhow::Error;

#[derive(Debug)]
pub struct Config {
    pub from_format: Format,
    pub to_format: Format,
    pub input_file: String,
    pub output_file: Option<String>,
    pub include_columns: Option<Vec<String>>,
    pub exclude_columns: Option<Vec<String>>,
}

impl Config {
    pub fn new(
        from_format: Format,
        to_format: Format,
        input_file: String,
        output_file: Option<String>,
        include_columns: Option<Vec<String>>,
        exclude_columns: Option<Vec<String>>,
    ) -> Self {
        Self {
            from_format,
            to_format,
            input_file,
            output_file,
            include_columns,
            exclude_columns,
        }
    }

    pub fn validate(&self) -> Result<(), Error> {
        if self.output_file.is_none() {
            anyhow::bail!("Output file must be specified via --output");
        }
        Ok(())
    }
}
