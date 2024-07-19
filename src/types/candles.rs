use crate::traits::AsDataFrame;
use chrono::NaiveDateTime;
use polars::frame::DataFrame;
use polars::prelude::{NamedFrom, Series};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};

/// Abstracts a candlestick
#[derive(Serialize, Debug, PartialEq)]
pub struct Candle {
    #[serde(serialize_with = "crate::serialization::naive_dt_serializer")]
    #[serde(deserialize_with = "crate::serialization::naive_dt_deserializer")]
    pub time: NaiveDateTime,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
}

impl<'de> Deserialize<'de> for Candle {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let arr = <[Decimal; 6]>::deserialize(deserializer)?;

        let time = NaiveDateTime::from_timestamp_opt(arr[0].to_i64().unwrap(), 0).unwrap();
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
            Series::new("open", vec![self.open.to_f64().unwrap()]),
            Series::new("high", vec![self.high.to_f64().unwrap()]),
            Series::new("low", vec![self.low.to_f64().unwrap()]),
            Series::new("close", vec![self.close.to_f64().unwrap()]),
            Series::new("volume", vec![self.volume.to_f64().unwrap()]),
        ])
        .unwrap()
    }
}

impl AsDataFrame for Vec<Candle> {
    fn as_dataframe(&self) -> DataFrame {
        let mut time = Vec::with_capacity(self.len());
        let mut open = Vec::with_capacity(self.len());
        let mut high = Vec::with_capacity(self.len());
        let mut low = Vec::with_capacity(self.len());
        let mut close = Vec::with_capacity(self.len());
        let mut volume = Vec::with_capacity(self.len());

        for candle in self {
            time.push(candle.time);
            open.push(candle.open.to_f64().unwrap());
            high.push(candle.high.to_f64().unwrap());
            low.push(candle.low.to_f64().unwrap());
            close.push(candle.close.to_f64().unwrap());
            volume.push(candle.volume.to_f64().unwrap());
        }

        DataFrame::new(vec![
            Series::new("time", time),
            Series::new("open", open),
            Series::new("high", high),
            Series::new("low", low),
            Series::new("close", close),
            Series::new("volume", volume),
        ])
        .unwrap()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::Utc;
    use polars::prelude::AnyValue;
    use rust_decimal_macros::dec;

    #[test]
    fn test_as_dataframe() {
        let time = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap();
        let candle = Candle {
            time,
            open: dec!(1.0),
            high: dec!(2.0),
            low: dec!(3.0),
            close: dec!(4.0),
            volume: dec!(5.0),
        };
        let df = candle.as_dataframe();
        assert_eq!(df.shape(), (1, 6));
        assert_eq!(
            df.get_column_names(),
            &["time", "open", "high", "low", "close", "volume"]
        );
        assert_eq!(
            df.column("time")
                .unwrap()
                .datetime()
                .unwrap()
                .get(0)
                .unwrap(),
            time.timestamp_millis()
        );
        assert_eq!(
            df.column("open").unwrap().get(0).unwrap(),
            AnyValue::Float64(candle.open.to_f64().unwrap())
        );
        assert_eq!(
            df.column("high").unwrap().get(0).unwrap(),
            AnyValue::Float64(candle.high.to_f64().unwrap())
        );
        assert_eq!(
            df.column("low").unwrap().get(0).unwrap(),
            AnyValue::Float64(candle.low.to_f64().unwrap())
        );
        assert_eq!(
            df.column("close").unwrap().get(0).unwrap(),
            AnyValue::Float64(candle.close.to_f64().unwrap())
        );
        assert_eq!(
            df.column("volume").unwrap().get(0).unwrap(),
            AnyValue::Float64(candle.volume.to_f64().unwrap())
        );
    }

    #[test]
    fn test_vec_of_candles_to_dataframe() {
        // generate some candles
        let mut candles = Vec::new();
        for i in 0..10 {
            let _i = Decimal::from(i);
            let time = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp() + i, 0).unwrap();

            let open = dec!(1.0);
            let high = dec!(2.0);
            let low = dec!(3.0);
            let close = dec!(4.0);
            let volume = dec!(5.0);

            let candle = Candle {
                time,
                open: open + _i,
                high: high + _i,
                low: low + _i,
                close: close + _i,
                volume: volume + _i,
            };
            candles.push(candle);
        }
        let df = candles.as_dataframe();
        assert_eq!(df.shape(), (10, 6));
        assert_eq!(
            df.get_column_names(),
            &["time", "open", "high", "low", "close", "volume"]
        );
        for i in 0..10 {
            assert_eq!(
                df.column("time")
                    .unwrap()
                    .datetime()
                    .unwrap()
                    .get(i)
                    .unwrap(),
                candles[i].time.timestamp_millis()
            );
            assert_eq!(
                df.column("open").unwrap().get(i).unwrap(),
                AnyValue::Float64(candles[i].open.to_f64().unwrap())
            );
            assert_eq!(
                df.column("high").unwrap().get(i).unwrap(),
                AnyValue::Float64(candles[i].high.to_f64().unwrap())
            );
            assert_eq!(
                df.column("low").unwrap().get(i).unwrap(),
                AnyValue::Float64(candles[i].low.to_f64().unwrap())
            );
            assert_eq!(
                df.column("close").unwrap().get(i).unwrap(),
                AnyValue::Float64(candles[i].close.to_f64().unwrap())
            );
            assert_eq!(
                df.column("volume").unwrap().get(i).unwrap(),
                AnyValue::Float64(candles[i].volume.to_f64().unwrap())
            );
        }
    }
}
