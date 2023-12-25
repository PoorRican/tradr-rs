/// This module contains traits for calculating indicator data from signals and determining
/// signal data from that calculated data. Additionally, implementations of these traits
/// are provided for specific indicators.
///
/// For each indicator implementation, there are two main traits that are implemented:
/// 1. [`IndicatorGraphHandler`] - This trait is used to calculate the indicator data from the candle data.
/// 2. [`IndicatorSignalHandler`] - This trait is used to calculate the signal data from the indicator data.
///
/// While these traits are inherently interlinked, they have been coded separately to allow for
/// more flexibility in the future, and easier testing. The [`Indicator`] trait is a combination of
/// the [`IndicatorGraphHandler`] and [`IndicatorSignalHandler`] traits and is intended as the primary interface
/// for processing candle data.
///
/// For all three traits, there are two main interfaces for processing candle data:
/// 1. All candle data is processed at once, and the output is stored in a time-series DataFrame. This
///     is intended for bootstrapping historical candle data and for backtesting.
/// 2. A new candle row is processed, and the output is appended to the existing time-series DataFrame.
///     This is intended for processing new candle data as it is received.
///
/// # Notes
/// Due to the nature of candle data as it is received, there is no sorting that is performed internally.
pub mod bbands;

use chrono::NaiveDateTime;
use polars::prelude::{DataFrame, DataFrameJoinOps, JoinArgs, JoinType};
use crate::types::Signal;

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
    fn convert_output_to_dataframe(
        &self,
        output: Self::Output,
        timestamp: NaiveDateTime,
    ) -> DataFrame;
}

/// This trait processes the candle data using the indicator function, then the output (the "graph")
/// is stored in a time-series DataFrame.
///
/// There are main interfaces for processing the candle data:
/// 1. All candle data is processed at once, and the entire output is stored in a time-series DataFrame
/// 2. A new candle row is processed, and the output is appended to the time-series DataFrame
trait IndicatorGraphHandler: IndicatorUtilities {
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
    /// # Arguments
    /// * `row` - A single row containing the new candle data
    ///
    /// # Panics
    /// * If the DataFrame does not contain exactly one new row
    fn process_new_candles(&mut self, row: &DataFrame);

    /// Get the entirety of the calculated indicator data
    ///
    /// # Returns
    /// A reference to the internal indicator graph
    fn get_indicator_history(&self) -> &Option<DataFrame>;
}

trait IndicatorSignalHandler: IndicatorGraphHandler {
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
    /// # Arguments
    /// * `row` - A single row containing the new candle data
    ///
    /// # Panics
    /// * If the DataFrame does not contain exactly one new row
    fn process_new_data(&mut self, row: &DataFrame);

    /// Get the entirety of the calculated signal data
    ///
    /// # Returns
    /// A reference to the internal signal data dataframe
    fn get_signal_history(&self) -> &Option<DataFrame>;
}

/// This trait combines the [`IndicatorGraphHandler`] and [`IndicatorSignalHandler`] traits and is intended
/// as the primary interface exposed for processing candle data.
///
/// # Sequence of Operations
///
/// For normal runtime, the sequence of operations is as follows:
/// 1. [`Indicator::process_existing()`] is called to process historical candle data at the beginning of the runtime.
/// 2. [`Indicator::process_new()`] is called to process new candle data as it is received from the market.
/// 3. [`Indicator::get_last_signal()`] is called to determine whether to attempt a trade.
///
/// For backtesting, the sequence of operations is as follows:
/// 1. [`Indicator::process_existing()`] is called to process historical candle data.
/// 2. [`Indicator::get_signals()`] is called to get all of the processed signal history.
pub trait Indicator: IndicatorGraphHandler + IndicatorSignalHandler {

    /// Process existing candle data
    ///
    /// This is the main interface for processing existing candle data. It is meant to be called once
    /// at the beginning of the runtime for bootstrapping historical data, or for backtesting.
    ///
    /// # Arguments
    /// * `candles` - Historical candle data
    fn process_existing(&mut self, candles: &DataFrame) {
        self.process_existing_candles(candles);
        self.process_existing_data(candles);
    }

    /// Process new candle data
    ///
    /// This is the main interface for processing new candle data. It is meant to be called with
    /// new candle data as it is received from the market.
    ///
    /// # Arguments
    /// * `row` - New candle data containing exactly one new row
    ///
    /// # Panics
    /// * If the DataFrame does not contain exactly one new row
    fn process_new(&mut self, row: &DataFrame) {
        assert_eq!(row.height(), 1, "DataFrame must contain exactly one new row");
        self.process_new_candles(row);
        self.process_new_data(row);
    }

    /// Get the last signal
    ///
    /// Sort is not internally guaranteed.
    ///
    /// # Returns
    /// * `Some` - The last signal in the signal history
    /// * `None` - If there is no signal history
    fn get_last_signal(&self) -> Option<Signal> {
        if let Some(signal_history) = self.get_signal_history() {
            let last_row = signal_history
                .tail(Some(1));
            let signal_val = last_row
                .column("signal")
                .unwrap()
                .i32()
                .unwrap()
                .get(0)
                .unwrap();
            Some(Signal::from(signal_val))
        } else {
            None
        }
    }

    /// Get signal history
    ///
    /// Exposes internal signal history for debugging or backtesting purposes.
    ///
    /// # Returns
    /// * `Some` - The raw signal history. This is a time-series [`DataFrame`] with the columns "time" and "signal".
    ///     Values for "signal" are not converted to the [`Signal`] enum.
    /// * `None` - If there is no signal history
    fn get_signals(&self) -> &Option<DataFrame> {
        self.get_signal_history()
    }
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
fn extract_new_rows(updated: &DataFrame, data: &DataFrame) -> DataFrame {
    // perform an anti-join to get the new rows
    updated
        .join(data, ["time"], ["time"], JoinArgs::new(JoinType::Anti))
        .unwrap()
}

#[cfg(test)]
mod tests {
    use crate::indicators::extract_new_rows;
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
