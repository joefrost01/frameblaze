//! row_filter.rs
use super::Transform;
use anyhow::{bail, Result};
use polars::prelude::*;

/// Comparison operators for row filtering.
#[derive(Debug, Clone, Copy)]
pub enum RowFilterOp {
    Eq, // equals
    Gt, // greater than
    Lt, // less than
}

/// The type of value we're comparing against:
/// - `Int(i64)` for integer comparisons
/// - `Str(String)` for string comparisons
#[derive(Debug, Clone)]
pub enum RowFilterValue {
    Int(i64),
    Str(String),
}

/// A RowFilter that keeps only rows where `column` <op> `value`.
#[derive(Debug, Clone)]
pub struct RowFilter {
    pub column: String,
    pub op: RowFilterOp,
    pub value: RowFilterValue,
}

impl RowFilter {
    /// Creates a new RowFilter for a given column, operator, and comparison value.
    /// Example: `RowFilter::new("age", RowFilterOp::Gt, RowFilterValue::Int(30))`
    pub fn new<S: Into<String>>(column: S, op: RowFilterOp, value: RowFilterValue) -> Self {
        Self {
            column: column.into(),
            op,
            value,
        }
    }
}

/// Implement the same `Transform` trait as your ColumnFilter uses.
impl Transform for RowFilter {
    fn transform(&self, df: DataFrame) -> Result<DataFrame> {
        // Build a Polars expression for the filter
        let filter_expr = match (&self.op, &self.value) {
            // integer comparisons
            (RowFilterOp::Eq, RowFilterValue::Int(i)) => col(&self.column).eq(lit(*i)),
            (RowFilterOp::Gt, RowFilterValue::Int(i)) => col(&self.column).gt(lit(*i)),
            (RowFilterOp::Lt, RowFilterValue::Int(i)) => col(&self.column).lt(lit(*i)),

            // string equality
            (RowFilterOp::Eq, RowFilterValue::Str(s)) => col(&self.column).eq(lit(s.as_str())),

            // not implementing string '>' or '<' for now
            (RowFilterOp::Gt, RowFilterValue::Str(_)) => {
                bail!("String '>' comparison not implemented")
            }
            (RowFilterOp::Lt, RowFilterValue::Str(_)) => {
                bail!("String '<' comparison not implemented")
            }
        };

        // Use lazy mode to apply the filter, then collect an eager DataFrame
        let filtered = df.lazy().filter(filter_expr).collect()?;
        Ok(filtered)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transform::Transform; // or wherever your Transform trait is defined
    use polars::prelude::{DataFrame, df};

    /// A helper to build a test DataFrame with i64 integers (to avoid type mismatches).
    fn sample_df() -> DataFrame {
        (df! {
            "name" => &["Alice", "Bob", "Charlie"],
            // specify i64 suffix so polars infers Int64
            "age"  => &[30i64, 25i64, 35i64],
            "city" => &["NYC", "LA", "CHI"]
        }).unwrap()
    }

    #[test]
    fn test_eq_int() {
        let df = sample_df();
        // Keep rows where age == 25 -> "Bob"
        let filter = RowFilter::new("age", RowFilterOp::Eq, RowFilterValue::Int(25));
        let out = filter.transform(df).unwrap();
        assert_eq!(out.shape(), (1, 3)); // 1 row, 3 columns

        // check row[0, "name"] => "Bob"
        let val = out.column("name").unwrap().get(0).unwrap().to_string();
        assert_eq!(val, "\"Bob\"");
    }

    #[test]
    fn test_gt_int() {
        let df = sample_df();
        // Keep rows where age > 30 -> "Charlie"
        let filter = RowFilter::new("age", RowFilterOp::Gt, RowFilterValue::Int(30));
        let out = filter.transform(df).unwrap();
        assert_eq!(out.shape(), (1, 3));

        // row[0, "name"] => "Charlie"
        let val = out.column("name").unwrap().get(0).unwrap().to_string();
        assert_eq!(val, "\"Charlie\"");
    }

    #[test]
    fn test_eq_str() {
        let df = sample_df();
        // Keep rows where city == "LA" -> "Bob"
        let filter = RowFilter::new("city", RowFilterOp::Eq, RowFilterValue::Str("LA".into()));
        let out = filter.transform(df).unwrap();
        assert_eq!(out.shape(), (1, 3));

        let val_city = out.column("city").unwrap().get(0).unwrap().to_string();
        let val_name = out.column("name").unwrap().get(0).unwrap().to_string();
        assert_eq!(val_city, "\"LA\"");
        assert_eq!(val_name, "\"Bob\"");
    }

    #[test]
    fn test_no_matches() {
        let df = sample_df();
        // keep rows where age == 99 => none
        let filter = RowFilter::new("age", RowFilterOp::Eq, RowFilterValue::Int(99));
        let out = filter.transform(df).unwrap();
        assert_eq!(out.shape(), (0, 3));
    }
}
