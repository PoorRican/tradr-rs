use chrono::NaiveDateTime;
use polars::frame::DataFrame;
use polars::prelude::{NamedFrom, Series};
use serde::{Deserialize, Serialize};
use crate::traits::AsDataFrame;


/// Abstracts a candlestick
#[derive(Serialize, Debug, PartialEq)]
pub struct Candle {
    #[serde(serialize_with = "crate::time::naive_dt_serializer")]
    #[serde(deserialize_with = "crate::time::naive_dt_deserializer")]
    pub time: NaiveDateTime,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

impl<'de> Deserialize<'de> for Candle {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
    {
        let arr = <[f64; 6]>::deserialize(deserializer)?;

        let time = NaiveDateTime::from_timestamp_opt(arr[0] as i64, 0).unwrap();
        let open = arr[1];
        let high = arr[2];
        let low = arr[3];
        let close = arr[4];
        let volume = arr[5];

        Ok(Candle {
            time,
            open,
            high,
            low,
            close,
            volume,
        })
    }
}


impl AsDataFrame for Candle {
    fn as_dataframe(&self) -> DataFrame {
        DataFrame::new(vec![
            Series::new("time", vec![self.time]),
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
        let time = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap();
        let candle = Candle {
            time,
            open: 1.0,
            high: 2.0,
            low: 3.0,
            close: 4.0,
            volume: 5.0,
        };
        let df = candle.as_dataframe();
        assert_eq!(df.shape(), (1, 6));
        assert_eq!(df.get_column_names(), &["time", "open", "high", "low", "close", "volume"]);
        assert_eq!(df.column("time").unwrap().datetime().unwrap().get(0).unwrap(),
                   time.timestamp_millis());
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