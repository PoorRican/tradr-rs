use polars::prelude::*;
use crate::indicators::GraphProcessingError;
use crate::processor::CandleProcessor;
use crate::types::Signal;

/// The Volume Weighted Average Price (VWAP) indicator
pub struct VWAP {
    /// The window size for the VWAP calculation
    window: usize,
}

impl VWAP {
    pub fn new(window: usize) -> Self {
        Self {
            window,
        }
    }
}

impl CandleProcessor for VWAP {
    type ReturnType = Signal;
    type ErrorType = GraphProcessingError;

    fn process_candle(&self, candles: &DataFrame) -> Result<Self::ReturnType, Self::ErrorType> {
        let graph = calculate_vwap(candles, self.window)
            .map_err(|e| GraphProcessingError::DataFrameError(e))?;

        let current_price = candles.column("close").unwrap().f64().unwrap().get(candles.height() - 1).unwrap();

        let last_vwap = graph.column("vwap").unwrap().f64().unwrap().get(graph.height() - 1).unwrap();

        let signal = if current_price > last_vwap {
            Signal::Buy
        } else if current_price < last_vwap {
            Signal::Sell
        } else {
            Signal::Hold
        };
        Ok(signal)
    }

    fn get_name(&self) -> &'static str {
        "vwap"
    }
}

fn calculate_vwap(candles: &DataFrame, window: usize) -> Result<DataFrame, PolarsError> {
    // Calculate typical price: (high + low + close) / 3
    let df = candles
        .tail(Some(window))
        .lazy()
        .select([
            col("time"),
            col("volume"),
            ((col("high")
                + col("low")
                + col("close")
            ) / lit(3))
                .alias("typical_price")
        ]).collect()?;

    // Calculate cumulative typical price * volume and cumulative volume
    let df = df
        .lazy()
        .select([
            col("time"),
            col("volume").cum_sum(false).alias("cum_volume"),
            (col("typical_price") * col("volume")).cum_sum(false).alias("cum_tp_vol")
        ]).collect()?;

    // Calculate VWAP
    df.lazy()
        .select([
            col("time"),
            (col("cum_tp_vol") / col("cum_volume")).alias("vwap")
        ]).collect()
}