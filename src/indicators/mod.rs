mod bbands;

use chrono::NaiveDateTime;
use polars::prelude::{DataFrame, DataFrameJoinOps, JoinArgs, JoinType};


/// Internal functions for indicators
///
/// These functions are used to manage the stateful indicator object, and process the indicator
/// output a properly structured dataframe.
///
/// This interface should not be exposed to higher-level code.
trait IndicatorUtilities {
    type Output;

    /// Reset the indicator
    ///
    /// This is used to reset the indicator to its initial state since indicator functions used by
    /// the TA library are stateful.
    fn restart_indicator(&mut self);

    /// Convert the indicator output to a DataFrame
    ///
    /// This is used to convert the indicator output to a DataFrame with a single row.
    fn convert_output_to_dataframe(&self, output: Self::Output, timestamp: NaiveDateTime) -> DataFrame;
}

/// This trait processes the candle data using the indicator function, then the output (the "graph")
/// is stored in a time-series DataFrame.
///
/// There are main interfaces for processing the candle data:
/// 1. All candle data is processed at once, and the entire output is stored in a time-series DataFrame
/// 2. A new candle row is processed, and the output is appended to the time-series DataFrame
pub trait IndicatorGraphHandler: IndicatorUtilities {
    /// Process indicator data for all candle data
    ///
    /// This is called to "bootstrap" the indicator data. It is called once at the beginning of the
    /// runtime.
    ///
    /// Any old indicator data is cleared.
    ///
    /// # Arguments
    /// * `candles` - The DataFrame with the candle data
    fn process_existing_candles(&mut self, candles: &DataFrame);

    /// Update processed indicator data with new candle data rows
    ///
    /// Internally, `extract_new_rows()` is called to get the new candle data, then the new candle data is
    /// processed and appended to the time-series DataFrame
    ///
    /// # Arguments
    /// * `candles` - The DataFrame with the candle data. Must contain one new row.
    ///
    /// # Panics
    /// * If the DataFrame does not contain exactly one new row
    fn process_new_candles(&mut self, candles: &DataFrame);

    /// Get the entirety of the calculated indicator data
    ///
    /// # Returns
    /// A reference to the internal indicator graph
    fn get_indicator_history(&self) -> &Option<DataFrame>;
}


pub trait IndicatorSignalHandler: IndicatorGraphHandler {
    /// Process signal data for all candle data
    ///
    /// This is called to "bootstrap" the signal data, meant to be called once at the beginning of the
    /// runtime.
    ///
    /// Any old signal data is cleared.
    ///
    /// # Arguments
    /// * `candles` - The DataFrame with candle data. This is used to determine the signal.
    fn process_existing_data(&mut self, candles: &DataFrame);

    /// Update processed signal data with a new indicator graph row
    ///
    /// Internally, `extract_new_rows()` is called to get the new indicator graph row, then the row is
    /// processed and appended to the time-series DataFrame
    ///
    /// # Arguments
    /// * `candles` - The DataFrame with the candle data and is used to determine the signal.
    ///
    /// # Panics
    /// * If the DataFrame does not contain exactly one new row
    fn process_new_data(&mut self, candles: &DataFrame);

    /// Get the entirety of the calculated signal data
    ///
    /// # Returns
    /// A reference to the internal signal data dataframe
    fn get_signal_history(&self) -> &Option<DataFrame>;

}


/// Extract new rows from a time-series DataFrame
///
/// This performs an anti-join between two columns along the "time" column. The result is a DataFrame
/// with the rows that are in the `updated` DataFrame but not in the `data` DataFrame.
///
/// This function is used when extracting new candle data that has not been processed by the indicator,
/// and indicator data that has not been processed for signals.
///
/// # Arguments
/// * `updated` - The DataFrame with the new rows
/// * `data` - The DataFrame with the old rows
///
/// # Returns
/// A DataFrame with the new rows from `updated`
fn extract_new_rows(updated: &DataFrame, data: &DataFrame) -> DataFrame {
    // perform an anti-join to get the new rows
    updated.join(data, ["time"], ["time"], JoinArgs::new(JoinType::Anti)).unwrap()
}

#[cfg(test)]
mod tests {
    use polars::prelude::*;
    use crate::indicators::extract_new_rows;

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
        ).unwrap();

        let indicator_data = df!(
            "time" => &[1, 2, 3],
            "open" => &[1, 2, 3],
            "high" => &[1, 2, 3],
            "low" => &[1, 2, 3],
            "close" => &[1, 2, 3],
            "volume" => &[1, 2, 3],
        ).unwrap();

        let new_rows = extract_new_rows(&candles, &indicator_data);

        assert_eq!(new_rows.shape(), (2, 6));

        // check time column
        assert_eq!(new_rows.column("time").unwrap().i32().unwrap().get(0), Some(41));
        assert_eq!(new_rows.column("time").unwrap().i32().unwrap().get(1), Some(51));

        // check open column
        assert_eq!(new_rows.column("open").unwrap().i32().unwrap().get(0), Some(42));
        assert_eq!(new_rows.column("open").unwrap().i32().unwrap().get(1), Some(52));

        assert_eq!(new_rows.column("high").unwrap().i32().unwrap().get(0), Some(43));
        assert_eq!(new_rows.column("high").unwrap().i32().unwrap().get(1), Some(53));

        assert_eq!(new_rows.column("low").unwrap().i32().unwrap().get(0), Some(44));
        assert_eq!(new_rows.column("low").unwrap().i32().unwrap().get(1), Some(54));

        assert_eq!(new_rows.column("close").unwrap().i32().unwrap().get(0), Some(45));
        assert_eq!(new_rows.column("close").unwrap().i32().unwrap().get(1), Some(55));

        assert_eq!(new_rows.column("volume").unwrap().i32().unwrap().get(0), Some(46));
        assert_eq!(new_rows.column("volume").unwrap().i32().unwrap().get(1), Some(56));
    }
}