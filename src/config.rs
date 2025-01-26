use anyhow::Result;

#[derive(Debug, Clone)]
pub struct Config {
    pub from_format: String,
    pub to_format: String,
    pub input_file: String,
    pub output_file: Option<String>,

    pub include_columns: Option<Vec<String>>,
    pub exclude_columns: Option<Vec<String>>,
}

impl Config {
    // Build a config from CLI arguments. We'll do advanced merging in future versions.
    pub fn new(
        from_format: String,
        to_format: String,
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

    pub fn validate(&self) -> Result<()> {
        // Basic validation
        if !["csv", "parquet"].contains(&self.from_format.as_str()) {
            anyhow::bail!("Invalid 'from_format': {}", self.from_format);
        }
        if !["csv", "parquet"].contains(&self.to_format.as_str()) {
            anyhow::bail!("Invalid 'to_format': {}", self.to_format);
        }
        if self.output_file.is_none() {
            anyhow::bail!("Output file must be specified via --output");
        }
        Ok(())
    }
}
