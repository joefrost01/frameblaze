use super::Transform;
use anyhow::Result;
use polars::prelude::*;

#[derive(Debug, Clone)]
pub struct ColumnFilter {
    pub include: Option<Vec<String>>,
    pub exclude: Option<Vec<String>>,
}

impl ColumnFilter {
    pub fn new(include: Option<Vec<String>>, exclude: Option<Vec<String>>) -> Self {
        Self { include, exclude }
    }
}

impl Transform for ColumnFilter {
    fn transform(&self, mut df: DataFrame) -> Result<DataFrame> {
        // 1) If include is Some, select only those columns
        if let Some(cols) = &self.include {
            let column_names: Vec<String> = df
                .get_column_names()
                .iter()
                .map(|pl_s| pl_s.as_str().to_string())
                .filter(|name| cols.contains(name))
                .collect();

            df = df.select(column_names)?;
        }

        // 2) If exclude is Some, drop those columns
        if let Some(cols) = &self.exclude {
            for col in cols {
                // check if column name is in DataFrame
                let contained = df
                    .get_column_names()
                    .iter()
                    .any(|pl_s| pl_s.as_str() == col);
                if contained {
                    df = df.drop(col)?;
                }
            }
        }

        Ok(df)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::transform::Transform;

    fn sample_dataframe() -> DataFrame {
        df! {
            "name" => &["Alice", "Bob", "Charlie"],
            "age" => &[30, 25, 35],
            "city" => &["New York", "Los Angeles", "Chicago"]
        }.unwrap()
    }

    #[test]
    fn test_include_columns() {
        let df = sample_dataframe();
        let filter = ColumnFilter::new(Some(vec!["name".to_string(), "city".to_string()]), None);

        let result = filter.transform(df).unwrap();

        // Check if only the included columns are present
        assert_eq!(result.get_column_names(), vec!["name", "city"]);
    }

    #[test]
    fn test_exclude_columns() {
        let df = sample_dataframe();
        let filter = ColumnFilter::new(None, Some(vec!["age".to_string()]));

        let result = filter.transform(df).unwrap();

        // Check if the excluded column is missing
        assert_eq!(result.get_column_names(), vec!["name", "city"]);
    }

    #[test]
    fn test_include_and_exclude_columns() {
        let df = sample_dataframe();
        let filter = ColumnFilter::new(
            Some(vec!["name".to_string(), "age".to_string(), "city".to_string()]),
            Some(vec!["city".to_string()]),
        );

        let result = filter.transform(df).unwrap();

        // Check if the included columns minus the excluded ones are present
        assert_eq!(result.get_column_names(), vec!["name", "age"]);
    }

    #[test]
    fn test_include_nonexistent_columns() {
        let df = sample_dataframe();
        let filter = ColumnFilter::new(Some(vec!["nonexistent".to_string()]), None);

        let result = filter.transform(df);

        // The DataFrame should have 0 columns but retain its original number of rows
        assert!(result.is_ok());
        let result_df = result.unwrap();
        assert_eq!(result_df.shape(), (3, 0)); // 3 rows, 0 columns
    }

    #[test]
    fn test_all_columns_excluded() {
        let df = sample_dataframe();
        let filter = ColumnFilter::new(None, Some(vec!["name".to_string(), "age".to_string(), "city".to_string()]));

        let result = filter.transform(df).unwrap();

        // The DataFrame should have 0 columns but retain its original number of rows
        assert_eq!(result.get_column_names(), Vec::<&str>::new());
        assert_eq!(result.shape(), (3, 0)); // 3 rows, 0 columns
    }

    #[test]
    fn test_empty_include_and_exclude() {
        let df = sample_dataframe();
        let filter = ColumnFilter::new(None, None);

        let result = filter.transform(df).unwrap();

        // Original DataFrame should remain unchanged
        assert_eq!(result.get_column_names(), vec!["name", "age", "city"]);
    }
}
