/// This module contains traits for calculating indicator data from signals and determining
/// signal data from that calculated data. Additionally, implementations of these traits
/// are provided for specific indicators.
///
/// For each indicator implementation, there are two main traits that are implemented:
/// 1. [`IndicatorGraphHandler`] - This trait is used to calculate the indicator data from the candle data.
/// 2. [`IndicatorSignalHandler`] - This trait is used to calculate the signal data from the indicator data.
///
/// While these traits are inherently interlinked, they've been coded separately to allow for
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
mod bbands;
mod vwap;

// Re-exports
pub use bbands::BBands;
pub use vwap::VWAP;

use crate::processor::CandleProcessor;
use crate::types::Signal;
use polars::prelude::*;

#[deprecated(since = "0.5.0", note = "Create a new error enum")]
#[derive(Debug)]
pub enum SignalExtractionError {
    InvalidSeriesLength,
    InvalidGraphColumns,
    IndicesNotAligned,
    InvalidDataType,
    CandlesEmpty,
}

#[deprecated(since = "0.5.0", note = "Create a new error enum")]
#[derive(Debug)]
pub enum SignalProcessingError {
    GraphHistoryMissing,
    GraphHistoryBehindCandles,
    DuplicatedCandleTimestamps,
    GraphIndexNotAlignedWithCandles,
    ExtractionError(SignalExtractionError),
}

#[derive(Debug)]
pub enum GraphProcessingError {
    InvalidCandleColumns,
    InvalidGraphLength,
    CandlesEmpty,
    DataFrameError(PolarsError),
    InsufficientCandleData,
}

#[deprecated(since = "0.5.0", note = "Create a new error enum")]
#[derive(Debug)]
pub enum IndicatorProcessingError {
    GraphError(GraphProcessingError),
    SignalError(SignalProcessingError),
}

/// Internal functions for indicators
///
/// These functions are used to manage the stateful indicator object, and process the indicator
/// output a properly structured dataframe.
///
/// This interface should not be exposed to higher-level code.
trait IndicatorUtilities {
    /// Reset the indicator
    ///
    /// This is used to reset the indicator to its initial state since indicator functions used by
    /// the TA library are stateful.
    fn restart_indicator(&mut self);
}

/// This trait processes the candle data using the indicator function, then the output (the "graph")
/// is stored in a time-series DataFrame.
///
/// There are main interfaces for processing the candle data:
/// 1. All candle data is processed at once, and the entire output is stored in a time-series DataFrame
/// 2. A new candle row is processed, and the output is appended to the time-series DataFrame
trait IndicatorGraphHandler: IndicatorUtilities {
    /// Process indicator data and overwrite existing data
    ///
    /// This is meant to "bootstrap" the internal indicator graph with historical data.
    ///
    /// # Arguments
    /// * `candles` - The DataFrame with the candle data
    fn process_graph(&mut self, candles: &DataFrame) -> Result<(), GraphProcessingError>;

    /// Update processed indicator data with new candle data rows
    ///
    /// # Arguments
    /// * `candles` - New candle data. Should be larger than the window and must contain new data.
    ///
    /// # Panics
    /// * If the [`DataFrame`] only contains one new row, or does not contain new data.
    fn process_graph_for_new_candles(
        &mut self,
        candles: &DataFrame,
    ) -> Result<(), GraphProcessingError>;

    /// Get the entirety of the calculated indicator data
    ///
    /// # Returns
    /// A reference to the internal indicator graph
    fn get_indicator_history(&self) -> Option<&DataFrame>;
}

trait IndicatorSignalHandler: IndicatorGraphHandler {
    /// Process signal data and overwrite existing data
    ///
    /// This is meant to "bootstrap" the internal indicator graph with historical data.
    ///
    /// # Arguments
    /// * `candles` - The DataFrame with candle data. This is used to determine the signal.
    fn process_signals(&mut self, candles: &DataFrame) -> Result<(), SignalProcessingError>;

    /// Update processed signal data with a new indicator graph row
    ///
    /// # Arguments
    /// * `candles` - New candle data. Should be larger than the window and must contain new data.
    ///
    /// # Panics
    /// * If the [`DataFrame`] only contains one new row, or does not contain new data.
    fn process_signals_for_new_candles(
        &mut self,
        candles: &DataFrame,
    ) -> Result<(), SignalProcessingError>;

    /// Get the entirety of the calculated signal data
    ///
    /// # Returns
    /// A reference to the internal signal data dataframe
    fn get_signal_history(&self) -> Option<&DataFrame>;

    fn extract_signals(
        &self,
        graph: &DataFrame,
        candles: &DataFrame,
    ) -> Result<DataFrame, SignalExtractionError>;
}

/// This trait combines the [`IndicatorGraphHandler`] and [`IndicatorSignalHandler`] traits and is intended
/// as the primary interface exposed for processing candle data.
///
/// # Sequence of Operations
///
/// For normal runtime, the sequence of operations is as follows:
/// 1. [`Indicator::process_historical_candles()`] is called to process historical candle data at the beginning of the runtime.
/// 2. [`Indicator::process_new_candles()`] is called to process new candle data as it is received from the market.
/// 3. [`Indicator::get_last_signal()`] is called to determine whether to attempt a trade.
///
/// For backtesting, the sequence of operations is as follows:
/// 1. [`Indicator::process_historical_candles()`] is called to process historical candle data.
/// 2. [`Indicator::get_signals()`] is called to get all the processed signal history.
pub trait Indicator: IndicatorGraphHandler + IndicatorSignalHandler {
    fn get_name(&self) -> &'static str;

    /// Get the last signal
    ///
    /// Sort is not internally guaranteed.
    ///
    /// # Returns
    /// * `Some` - The last signal in the signal history
    /// * `None` - If there is no signal history
    fn get_last_signal(&self) -> Option<Signal> {
        if let Some(signal_history) = self.get_signal_history() {
            let last_row = signal_history.tail(Some(1));
            let signal_val = last_row
                .column("signal")
                .unwrap()
                .i8()
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
    fn get_signals(&self) -> Option<&DataFrame> {
        self.get_signal_history()
    }

    fn get_graph(&self) -> Option<&DataFrame> {
        self.get_indicator_history()
    }

    /// Save indicator graph as CSV
    fn save_graph_as_csv(&mut self, path: &str) -> Result<(), PolarsError>;
}
