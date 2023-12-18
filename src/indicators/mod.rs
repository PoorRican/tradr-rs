use chrono::{DateTime, Utc};
use polars::prelude::DataFrame;

use crate::types::{
    signals::Signal,
    time::Timestamp
};

pub trait Indicator {

    /// Get the internal candle time-series reference
    fn get_candles(&self) -> &DataFrame;

    /// Set the internal candle time-series reference
    ///
    /// If this is set, both indicator and signal data must be recalculated
    fn set_candles(&mut self, candles: &DataFrame);

    /// Get the entirety of the calculated indicator data
    fn get_indicator_data(&self) -> &DataFrame;

    /// Get the entirety of the calculated signal data
    fn get_calculated_signal_data(&self) -> &DataFrame;

    /// Get the signal at a given point in time
    ///
    /// This retrieves a single row from internal time-series data
    fn get_signal(&self, point: Option<DateTime<Utc>>) -> Option<Signal>;

    /// Determine the signal for a given point in time
    ///
    /// This updates internal signal time-series data
    fn determine_signal(&mut self, point: Option<Timestamp>);

    /// Calculate indicator and signal data
    fn calc_signals(&mut self);
}
