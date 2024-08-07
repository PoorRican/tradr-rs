use log::info;
use crate::indicators::GraphProcessingError;
use crate::types::Signal;
use polars::prelude::*;
use crate::processor::CandleProcessor;

const DEFAULT_PERIOD: usize = 20;
const DEFAULT_MULTIPLIER: f64 = 2.0;
const DEFAULT_THRESHOLD: f64 = 0.99;
const DEFAULT_SOURCE_COL_NAME: &str = "close";

#[derive(Debug, Clone)]
pub struct BBands {
    // Bollinger Bands parameters
    period: usize,
    multiplier: f64,

    // Indicator / signal parameters
    threshold: f64,
    source_column: String,
}

impl BBands {
    pub fn new(period: usize, multiplier: f64) -> Self {
        Self {
            period,
            multiplier,
            threshold: DEFAULT_THRESHOLD,
            source_column: String::from(DEFAULT_SOURCE_COL_NAME),
        }
    }

    pub fn with_threshold(mut self, threshold: f64) -> Self {
        self.threshold = threshold;
        self
    }

    pub fn with_source_column(mut self, source_column: String) -> Self {
        self.source_column = source_column;
        self
    }

    fn calculate_bollinger_bands(&self, df: &DataFrame) -> PolarsResult<DataFrame> {
        let index = df.column("time").unwrap();

        let series = df.column(self.source_column.as_str()).unwrap();

        let window = RollingOptionsFixedWindow {
            min_periods: self.period,
            window_size: self.period,
            ..Default::default()
        };

        let sma = series.rolling_mean(window.clone())?;
        let std_dev = series.rolling_std(window)?;

        let upper = sma.clone() + (&std_dev * self.multiplier);
        let lower = sma.clone() - (&std_dev * self.multiplier);

        df![
            "time" => index,
            "lower" => lower.unwrap().into_series(),
            "middle" => sma,
            "upper" => upper.unwrap().into_series()
        ]
    }

    /// Calculate signal from indicator graph and candle data
    ///
    /// This function uses a threshold to determine where the close price is relative to the bounds of the
    /// Bollinger Bands.
    ///
    /// # Arguments
    /// * `graph` - A subset of the indicator graph
    /// * `candles` - Candle data
    ///
    /// # Returns
    /// A DataFrame with time and signals columns
    fn extract_signal(
        &self,
        graph: &DataFrame,
        candles: &DataFrame,
    ) -> Result<Signal, GraphProcessingError> {
        let graph = graph.tail(Some(1));

        let lower = graph.column("lower").unwrap().f64().unwrap().get(0).unwrap();
        let middle = graph.column("middle").unwrap().f64().unwrap().get(0).unwrap();
        let upper = graph.column("upper").unwrap().f64().unwrap().get(0).unwrap();

        let candle_price = candles
            .column(DEFAULT_SOURCE_COL_NAME)
            .unwrap()
            .f64()
            .unwrap()
            .tail(Some(1))
            .get(0)
            .unwrap();

        let buy_threshold = middle - (middle - lower) * self.threshold;
        let sell_threshold = middle + (upper - middle) * self.threshold;

        if candle_price < buy_threshold {
            Ok(Signal::Buy)
        } else if candle_price > sell_threshold {
            Ok(Signal::Sell)
        } else {
            Ok(Signal::Hold)
        }
    }
}

impl Default for BBands {
    fn default() -> Self {
        Self::new(DEFAULT_PERIOD, DEFAULT_MULTIPLIER)
    }
}


impl CandleProcessor for BBands {
    type ReturnType = Signal;
    type ErrorType = GraphProcessingError;

    fn process_candle(&self, candles: &DataFrame) -> Result<Self::ReturnType, Self::ErrorType> {
        if candles.height() < self.period {
            return Ok(Signal::Hold);
        }

        // check validity of row
        if candles.get_column_names() != ["time", "open", "high", "low", "close", "volume"] {
            return Err(GraphProcessingError::InvalidCandleColumns);
        }

        // recalculate bollinger bands for a limited subset
        let last = candles.tail(Some(self.period));
        let calculated_graph = self.calculate_bollinger_bands(&last).unwrap();

        self.extract_signal(&calculated_graph, candles)
    }

    fn get_name(&self) -> &'static str {
        "bbands"
    }

    fn get_raw_dataframe(&self, candles: &DataFrame) -> DataFrame {
        info!("Calculating Bollinger Bands");

        self.calculate_bollinger_bands(candles).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use polars::prelude::*;

    #[test]
    fn test_new() {
        let bb = super::BBands::new(15, 4.0);
        assert_eq!(bb.period, 15);
        assert_eq!(bb.multiplier, 4.0);
    }

    #[test]
    fn test_default() {
        let bb = super::BBands::default();
        assert_eq!(bb.period, 20);
        assert_eq!(bb.multiplier, 2.0);
    }
}
