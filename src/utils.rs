use crate::types::{Candle, Side, Signal};
use chrono::{DateTime, NaiveDateTime};
use log::info;
use polars::error::PolarsResult;
use polars::prelude::*;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use sqlite::Connection;
use std::env::temp_dir;
use std::fs::{create_dir_all, remove_dir_all};
use std::path::{Path, PathBuf};

/// create temp dir for testing
pub fn create_temp_dir(dir: &Path) -> PathBuf {
    let temp_dir = temp_dir();
    let path = temp_dir.join(dir);

    // delete dir if it already exists
    if path.exists() {
        remove_dir_all(&path).unwrap();
    }
    create_dir_all(path.clone()).unwrap();
    path
}

/// Extract new rows from a time-series DataFrame
///
/// This performs an anti-join between two columns along the "time" column. The result is a DataFrame
/// with the rows that are in the `updated` DataFrame but not in the `data` DataFrame.
///
/// This function is used when extracting new candle data that has not been processed by the indicator,
/// and indicator data that has not been processed for signals. For example, if `updated` has two rows
/// (with times "1" and "2") and `data` has one row (with time "1"), then the result will be a DataFrame with
/// one row (corresponding to time "2"). Comparison is exclusively done on the "time" column and the content
/// of the other columns is ignored.
///
/// # Arguments
/// * `updated` - The DataFrame with the new rows
/// * `data` - The DataFrame with the old rows
///
/// # Returns
/// A DataFrame with the new rows from `updated`
pub fn extract_new_rows(updated: &DataFrame, data: &DataFrame) -> DataFrame {
    // perform an anti-join to get the new rows
    updated
        .join(data, ["time"], ["time"], JoinArgs::new(JoinType::Anti))
        .unwrap()
}

pub fn extract_candles_from_db(db_path: &str, table_name: &str) -> Result<Vec<Candle>, ()> {
    let conn = Connection::open(db_path).unwrap();

    let query = format!("SELECT * FROM {}", table_name);
    let results = conn
        .prepare(query)
        .unwrap()
        .into_iter()
        .map(|row| {
            let data = row.unwrap();
            Candle {
                time: DateTime::from_timestamp_millis(data.read::<i64, _>(0))
                    .unwrap()
                    .naive_utc(),
                high: Decimal::from_f64(data.read::<f64, _>(1)).unwrap(),
                low: Decimal::from_f64(data.read::<f64, _>(2)).unwrap(),
                open: Decimal::from_f64(data.read::<f64, _>(3)).unwrap(),
                close: Decimal::from_f64(data.read::<f64, _>(4)).unwrap(),
                volume: Decimal::from_f64(data.read::<f64, _>(5)).unwrap(),
            }
        })
        .collect::<Vec<_>>();
    Ok(results)
}

pub fn extract_candles_from_df(df: &DataFrame) -> PolarsResult<Vec<Candle>> {
    let time = df.column("time")?.datetime()?;
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    let open = df.column("open")?.f64()?;
    let close = df.column("close")?.f64()?;
    let volume = df.column("volume")?.f64()?;

    Ok((0..time.len())
        .into_iter()
        .map(|i| Candle {
            time: DateTime::from_timestamp_millis(time.get(i).unwrap())
                .unwrap()
                .naive_utc(),
            high: Decimal::from_f64(high.get(i).unwrap()).unwrap(),
            low: Decimal::from_f64(low.get(i).unwrap()).unwrap(),
            open: Decimal::from_f64(open.get(i).unwrap()).unwrap(),
            close: Decimal::from_f64(close.get(i).unwrap()).unwrap(),
            volume: Decimal::from_f64(volume.get(i).unwrap()).unwrap(),
        })
        .collect())
}

pub fn extract_signals_from_df(df: &DataFrame, column_name: &str) -> PolarsResult<Vec<Signal>> {
    Ok(df
        .column(column_name)?
        .i8()?
        .into_iter()
        .map(|value| {
            if let Some(value) = value {
                return Signal::from(value);
            } else {
                return Signal::Hold;
            }
        })
        .collect())
}

pub fn extract_side_from_df(df: &DataFrame, column_name: &str) -> PolarsResult<Vec<Side>> {
    Ok(df
        .column(column_name)?
        .i8()?
        .into_iter()
        .map(|value| Side::from(value.unwrap()))
        .collect())
}

/// Uses the `info!` macro to print the start and end time of the candles
pub fn print_candle_statistics(candles: &DataFrame) {
    let candle_start = candles
        .column("time")
        .unwrap()
        .datetime()
        .unwrap()
        .head(Some(1))
        .get(0)
        .unwrap();
    let candle_start = DateTime::from_timestamp_millis(candle_start)
        .unwrap()
        .naive_utc();
    let candle_end = candles
        .column("time")
        .unwrap()
        .datetime()
        .unwrap()
        .tail(Some(1))
        .get(0)
        .unwrap();
    let candle_end = DateTime::from_timestamp_millis(candle_end)
        .unwrap()
        .naive_utc();

    info!("Candles range: {:?} - {:?}", candle_start, candle_end);
}

#[derive(Debug)]
pub enum AlignmentError {
    DifferentLengths,
    TimestampsNotAligned,
}

pub fn check_candle_alignment(a: &DataFrame, b: &DataFrame) -> Result<(), AlignmentError> {
    // ensure that the market data and historical data are sorted by timestamp
    let market_data_index = a.column("time").unwrap().datetime().unwrap();
    let historical_data_index = b.column("time").unwrap().datetime().unwrap();
    if market_data_index.len() != historical_data_index.len() {
        return Err(AlignmentError::DifferentLengths);
    }
    let index_alignment_mask: Vec<bool> = market_data_index
        .iter()
        .zip(historical_data_index.iter())
        .map(|(a, b)| a != b)
        .collect();
    if index_alignment_mask.iter().any(|&x| x) {
        return Err(AlignmentError::TimestampsNotAligned);
    }

    Ok(())
}

pub fn trim_candles(candles: &DataFrame, end_time: NaiveDateTime, length: IdxSize) -> DataFrame {
    candles
        .clone()
        .lazy()
        .filter(col("time").lt(lit(end_time)))
        .tail(length)
        .collect()
        .unwrap()
}

#[cfg(test)]
mod tests {
    use crate::utils::extract_new_rows;
    use polars::prelude::*;

    /// Test that extract_new_rows() returns the correct rows
    #[test]
    fn test_extract_new_rows() {
        let candles = df!(
            "time" => &[1, 2, 3, 41, 51],
            "open" => &[1, 2, 3, 42, 52],
            "high" => &[1, 2, 3, 43, 53],
            "low" => &[1, 2, 3, 44, 54],
            "close" => &[1, 2, 3, 45, 55],
            "volume" => &[1, 2, 3, 46, 56],
        )
        .unwrap();

        let indicator_data = df!(
            "time" => &[1, 2, 3],
            "open" => &[1, 2, 3],
            "high" => &[1, 2, 3],
            "low" => &[1, 2, 3],
            "close" => &[1, 2, 3],
            "volume" => &[1, 2, 3],
        )
        .unwrap();

        let new_rows = extract_new_rows(&candles, &indicator_data);

        assert_eq!(new_rows.shape(), (2, 6));

        // check time column
        assert_eq!(
            new_rows.column("time").unwrap().i32().unwrap().get(0),
            Some(41)
        );
        assert_eq!(
            new_rows.column("time").unwrap().i32().unwrap().get(1),
            Some(51)
        );

        // check open column
        assert_eq!(
            new_rows.column("open").unwrap().i32().unwrap().get(0),
            Some(42)
        );
        assert_eq!(
            new_rows.column("open").unwrap().i32().unwrap().get(1),
            Some(52)
        );

        assert_eq!(
            new_rows.column("high").unwrap().i32().unwrap().get(0),
            Some(43)
        );
        assert_eq!(
            new_rows.column("high").unwrap().i32().unwrap().get(1),
            Some(53)
        );

        assert_eq!(
            new_rows.column("low").unwrap().i32().unwrap().get(0),
            Some(44)
        );
        assert_eq!(
            new_rows.column("low").unwrap().i32().unwrap().get(1),
            Some(54)
        );

        assert_eq!(
            new_rows.column("close").unwrap().i32().unwrap().get(0),
            Some(45)
        );
        assert_eq!(
            new_rows.column("close").unwrap().i32().unwrap().get(1),
            Some(55)
        );

        assert_eq!(
            new_rows.column("volume").unwrap().i32().unwrap().get(0),
            Some(46)
        );
        assert_eq!(
            new_rows.column("volume").unwrap().i32().unwrap().get(1),
            Some(56)
        );
    }
}
