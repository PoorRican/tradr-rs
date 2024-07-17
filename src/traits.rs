use polars::prelude::DataFrame;

pub trait AsDataFrame {
    fn as_dataframe(&self) -> DataFrame;
}
