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
mod bbands;

// Re-exports
pub use bbands::BBands;

use polars::prelude::*;
use crate::types::Signal;

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
    /// Process indicator data for all candle data
    ///
    /// This is called to "bootstrap" the indicator data. It is called once at the beginning of the
    /// runtime.
    ///
    /// Any old indicator data is cleared.
    ///
    /// # Arguments
    /// * `candles` - The DataFrame with the candle data
    fn process_graph_for_existing(&mut self, candles: &DataFrame);

    /// Update processed indicator data with new candle data rows
    ///
    /// # Arguments
    /// * `candles` - New candle data. Should be larger than the window and must contain new data.
    ///
    /// # Panics
    /// * If the [`DataFrame`] only contains one new row, or does not contain new data.
    fn process_graph_for_new_candles(&mut self, candles: &DataFrame) -> Result<(), ()>;

    /// Get the entirety of the calculated indicator data
    ///
    /// # Returns
    /// A reference to the internal indicator graph
    fn get_indicator_history(&self) -> Option<&DataFrame>;
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
    fn process_signals_for_existing(&mut self, candles: &DataFrame);

    /// Update processed signal data with a new indicator graph row
    ///
    /// # Arguments
    /// * `candles` - New candle data. Should be larger than the window and must contain new data.
    ///
    /// # Panics
    /// * If the [`DataFrame`] only contains one new row, or does not contain new data.
    fn process_signals_for_new_candles(&mut self, candles: &DataFrame) -> Result<(), ()>;

    /// Get the entirety of the calculated signal data
    ///
    /// # Returns
    /// A reference to the internal signal data dataframe
    fn get_signal_history(&self) -> Option<&DataFrame>;
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
        self.process_graph_for_existing(candles);
        self.process_signals_for_existing(candles);
    }

    /// Process new candle data
    ///
    /// This is the main interface for processing new candle data. It is meant to be called with
    /// new candle data as it is received from the market.
    ///
    /// # Arguments
    /// * `candles` - New candle data. Should be larger than processing window.
    ///
    /// # Panics
    /// * If the DataFrame does not contain more than one row
    fn process_new(&mut self, candles: &DataFrame) -> Result<(), ()> {
        assert!(candles.height() > 1, "DataFrame must contain more than one row");

        self.process_graph_for_new_candles(candles)?;
        self.process_signals_for_new_candles(candles)?;

        Ok(())
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
    fn get_signals(&self) -> Option<&DataFrame> {
        self.get_signal_history()
    }
}
