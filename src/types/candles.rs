use polars::frame::DataFrame;
use polars::prelude::{NamedFrom, Series};
use crate::types::time::Timestamp;
use crate::traits::AsDataFrame;

/// Abstracts a candlestick
pub struct Candle {
    pub time: Timestamp,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

impl AsDataFrame for Candle {
    fn as_dataframe(&self) -> DataFrame {
        DataFrame::new(vec![
            Series::new("time", vec![self.time.timestamp()]),
            Series::new("open", vec![self.open]),
            Series::new("high", vec![self.high]),
            Series::new("low", vec![self.low]),
            Series::new("close", vec![self.close]),
            Series::new("volume", vec![self.volume]),
        ]).unwrap()
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use chrono::Utc;
    use polars::prelude::AnyValue;

    #[test]
    fn test_as_dataframe() {
        let candle = Candle {
            time: Utc::now(),
            open: 1.0,
            high: 2.0,
            low: 3.0,
            close: 4.0,
            volume: 5.0,
        };
        let df = candle.as_dataframe();
        assert_eq!(df.shape(), (1, 6));
        assert_eq!(df.get_column_names(), &["time", "open", "high", "low", "close", "volume"]);
        assert_eq!(
            df.column("time").unwrap().get(0).unwrap(),
            AnyValue::Int64(candle.time.timestamp()));
        assert_eq!(
            df.column("open").unwrap().get(0).unwrap(),
            AnyValue::Float64(candle.open));
        assert_eq!(
            df.column("high").unwrap().get(0).unwrap(),
            AnyValue::Float64(candle.high));
        assert_eq!(
            df.column("low").unwrap().get(0).unwrap(),
            AnyValue::Float64(candle.low));
        assert_eq!(
            df.column("close").unwrap().get(0).unwrap(),
            AnyValue::Float64(candle.close));
        assert_eq!(
            df.column("volume").unwrap().get(0).unwrap(),
            AnyValue::Float64(candle.volume));
    }
}