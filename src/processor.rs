use polars::prelude::DataFrame;

/// Common interface for objects which process candle data
///
/// Implemented by `Strategy` and `Indicator` objects
pub trait CandleProcessor {
    type ReturnType;
    type ErrorType;
    fn process_candle(&self, candles: &DataFrame) -> Result<Self::ReturnType, Self::ErrorType>;
    fn get_name(&self) -> &'static str;

    /// Get computed data for the given candles
    ///
    /// Strictly meant for debugging and graphing.
    fn get_raw_dataframe(&self, candles: &DataFrame) -> DataFrame;
}