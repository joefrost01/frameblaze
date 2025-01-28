#[cfg(test)]
pub mod helpers {
    use crate::from::FromFile;
    use crate::to::ToFile;
    use anyhow::Result;
    use polars::prelude::*;
    use std::error::Error;
    use std::io::Write;
    use tempfile::NamedTempFile;

    pub fn create_sample_df() -> Result<DataFrame> {
        Ok(df! {
            "name" => &["Alice", "Bob"],
            "age" => &[30, 25],
            "city" => &["New York", "Los Angeles"]
        }?)
    }

    pub fn create_temp_file() -> Result<NamedTempFile> {
        let temp_file = NamedTempFile::new()?;
        Ok(temp_file)
    }

    pub fn create_malformed_file() -> Result<NamedTempFile> {
        let mut temp_file = create_temp_file()?;
        writeln!(temp_file, "this is not valid data")?;
        Ok(temp_file)
    }

    pub fn assert_dataframes_equal(df1: &DataFrame, df2: &DataFrame) {
        assert!(df1.equals(df2));
        assert_eq!(df1.shape(), df2.shape());
        assert_eq!(df1.get_column_names(), df2.get_column_names());
    }


    pub fn test_write_read_compare<W, R>(writer: &W, read_fn: R, schema_only: bool) -> Result<()>
    where
        W: ToFile,
        R: Fn(&str) -> Result<DataFrame, Box<dyn Error>>,
    {
        let temp_file = NamedTempFile::new()?;
        let file_path = temp_file.path().to_str().unwrap();
        let df = create_sample_df()?;
        writer.write_data(file_path, &df, schema_only)?;
        let read_df = read_fn(file_path).unwrap();
        assert_dataframes_equal(&df, &read_df);
        Ok(())
    }


    pub fn test_write_then_read<WriterFn>(writer_fn: WriterFn, reader: Box<dyn FromFile>) -> Result<()>
    where
        WriterFn: FnOnce(&mut NamedTempFile, &DataFrame) -> Result<()>,
    {
        let mut temp_file = create_temp_file()?;
        let df = create_sample_df()?;
        writer_fn(&mut temp_file, &df)?;
        let read_df = reader.read_data(temp_file.path().to_str().unwrap())?;
        assert_dataframes_equal(&df, &read_df);
        Ok(())
    }


    /// Test that writing a DataFrame to a given `file_path` fails.
    pub fn test_write_should_fail<W>(writer: &W,
    ) where
        W: ToFile,
    {
        let file_path = "/nonexistent_dir/test";
        let df = df! {
            "name" => &["Alice", "Bob"],
            "age" => &[30, 25]
        }.unwrap();
        let result = writer.write_data(file_path, &df, false);
        assert!(result.is_err(), "Expected write to fail, but it succeeded.");
    }


    pub fn test_write_overwrite<W, R>(writer: &W, reader: R) -> Result<()>
    where
        W: ToFile,
        R: Fn(&str) -> Result<DataFrame, Box<dyn Error>>,
    {
        let temp_file = NamedTempFile::new()?;
        let file_path = temp_file.path().to_str().unwrap();
        let df_first = create_sample_df()?;
        writer.write_data(file_path, &df_first, false)?;
        let df_second = create_sample_df()?;
        writer.write_data(file_path, &df_second, false)?;
        let df_read = reader(file_path).unwrap();
        assert_dataframes_equal(&df_second, &df_read);
        Ok(())
    }

    pub fn test_write_empty_dataframe<W, R>(writer: &W, reader: R) -> Result<()>
    where
        W: ToFile,
        R: Fn(&str) -> Result<DataFrame, Box<dyn Error>>,
    {
        let temp_file = NamedTempFile::new()?;
        let file_path = temp_file.path().to_str().unwrap();
        let df = DataFrame::default();
        writer.write_data(file_path, &df, false).unwrap();
        let read_df = reader(file_path).unwrap();
        assert_eq!(read_df.height(), 0);
        assert_eq!(read_df.width(), 0);

        Ok(())
    }
}
