use polars::prelude::*;
use anyhow::Result;

pub trait Transform {
    fn transform(&self, df: DataFrame) -> Result<DataFrame>;
}

pub mod column_filter;
