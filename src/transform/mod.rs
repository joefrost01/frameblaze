use anyhow::Result;
use polars::prelude::*;

pub trait Transform {
    fn transform(&self, df: DataFrame) -> Result<DataFrame>;
}

pub mod column_filter;
pub mod row_filter;
