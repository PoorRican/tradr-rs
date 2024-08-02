use polars::prelude::DataFrame;

/// Common interface for objects which process candle data
///
/// Implemented by `Strategy` and `Indicator` objects
pub trait CandleProcessor {
    type ErrorType;
    fn process_historical_candles(&mut self, candles: &DataFrame) -> Result<(), Self::ErrorType>;
    fn process_new_candles(&mut self, candles: &DataFrame) -> Result<(), Self::ErrorType>;
}