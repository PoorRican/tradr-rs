use chrono::{DateTime, NaiveDateTime};
use log::info;
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

    fn get_raw_dataframe(&self, candles: &DataFrame) -> DataFrame {
        info!("Calculating VWAP");

        let total_rows = candles.height();

        // Calculate initial VWAP for the first window
        let mut result = calculate_vwap(&candles.head(Some(self.window)), self.window).unwrap();

        // Prepare vectors to store VWAP values and timestamps
        let mut vwap_values = result.column("vwap").unwrap().f64().unwrap().to_vec();
        let mut vwap_values = vwap_values.iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let mut timestamps = result.column("time").unwrap().datetime().unwrap().to_vec();
        let mut timestamps = timestamps.iter().map(|x| x.unwrap()).collect::<Vec<i64>>();

        // Calculate VWAP for the remaining data using a rolling window
        for i in self.window..total_rows {
            let window_start = i - self.window + 1;

            let window_df = candles.slice(window_start as i64, self.window);
            let window_vwap = calculate_vwap(&window_df, self.window).unwrap();

            let vwap_value = window_vwap.column("vwap").unwrap().f64().unwrap().get(self.window - 1).unwrap();
            let timestamp = window_vwap.column("time").unwrap().datetime().unwrap().get(self.window - 1).unwrap();

            vwap_values.push(vwap_value);
            timestamps.push(timestamp);
        }

        // convert timestamps to DateTime

        let timestamps = timestamps.iter().map(|x| DateTime::from_timestamp_millis(*x).unwrap().naive_utc()).collect::<Vec<NaiveDateTime>>();

        // Create a new DataFrame with the calculated VWAP values
        DataFrame::new(vec![
            Series::new("time", timestamps),
            Series::new("vwap", vwap_values),
        ]).unwrap()
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