mod consensus;

use crate::indicators::GraphProcessingError;
use crate::markets::utils::save_candles;
use crate::processor::CandleProcessor;
pub use crate::strategies::consensus::Consensus;
use crate::types::Signal;
use log::info;
use polars::prelude::*;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub enum StrategyError {
    IndicatorError(GraphProcessingError),
}

/// A [`IndicatorContainer`] is a collection of [`CandleProcessor`] objects.
type IndicatorContainer =
    Vec<Box<dyn CandleProcessor<ErrorType = GraphProcessingError, ReturnType = Signal>>>;

/// A [`Strategy`] is a facade for interfacing with more than one [`CandleProcessor`] objects.
///
/// A simple interface is provided for bootstrapping historical candle data, processing new candle data,
/// and generating a consensus [`Signal`] among all [`CandleProcessor`] objects.
///
/// `Strategy::process_historical_candles` is used for bootstrapping the indicators with historical data.
/// `Strategy::process_new_candles` is used for ingesting new candle data and generating a consensus signal.
pub struct Strategy {
    pub indicators: IndicatorContainer,
    consensus: Consensus,
}

impl CandleProcessor for Strategy {
    type ReturnType = Signal;
    type ErrorType = StrategyError;

    /// Process all historical data
    ///
    /// The internal state of all [`CandleProcessor`] objects is updated with the historical data. Any existing
    /// data in the indicators is overwritten.
    ///
    /// This method is used upon initial load, or during backtesting.
    fn process_candle(&self, candles: &DataFrame) -> Result<Self::ReturnType, Self::ErrorType> {
        let results = self.indicators.iter().map(|indicator| {
            indicator
                .process_candle(candles)
                .map_err(|x| StrategyError::IndicatorError(x))
                .unwrap()
        });
        Ok(self.consensus.reduce(results))
    }

    fn get_name(&self) -> &'static str {
        "strategy"
    }

    fn get_raw_dataframe(&self, candles: &DataFrame) -> DataFrame {
        info!("Processing indicators for {} rows", candles.height());

        let graphs = self
            .indicators
            .iter()
            .map(|indicator| indicator.get_raw_dataframe(candles))
            .collect::<Vec<DataFrame>>();

        let mut df = graphs.get(0).unwrap().clone();

        info!("Joining {} results", graphs.len());
        for idx in 1..graphs.len() {
            df = df
                .lazy()
                .join(
                    graphs.get(idx).unwrap().clone().lazy(),
                    [col("time")],
                    [col("time")],
                    JoinArgs::new(JoinType::Left),
                )
                .collect()
                .unwrap();
        }

        df
    }
}

impl Strategy {
    pub fn new(indicators: IndicatorContainer, consensus: Consensus) -> Self {
        Self {
            indicators,
            consensus,
        }
    }

    pub fn save_indicators<P: Into<PathBuf>>(&self, candles: &DataFrame, path: P) {
        let path = path.into();

        if path.is_file() {
            panic!("Path is a file, expected a directory");
        } else if !path.exists() {
            std::fs::create_dir_all(&path).unwrap();
        }

        let mut df = self.get_raw_dataframe(candles);

        let joined_path = path.join("indicators.csv");
        let path = joined_path.to_str().unwrap();

        save_candles(&mut df, path).unwrap();
    }
}

#[cfg(test)]
mod strategy_tests {
    use super::*;
    use crate::indicators::BBands;
    use polars::prelude::*;

    fn setup_strategy_with_indicators() -> Strategy {
        let bbands = Box::new(BBands::default());
        Strategy::new(vec![bbands], Consensus::Majority)
    }

    fn create_dataframe() -> DataFrame {
        let dates = &["2021-01-01", "2021-01-02", "2021-01-03"];
        let opens = &[100.0, 200.0, 300.0];
        let closes = &[150.0, 250.0, 350.0];
        df![
            "time" => dates,
            "open" => opens,
            "close" => closes,
        ]
        .unwrap()
    }

    #[test]
    fn combined_signals_with_unanimous_buy_signals() {
        todo!()
    }

    #[test]
    fn combined_signals_with_unanimous_sell_signals() {
        todo!()
    }

    #[test]
    fn combined_signals_with_mixed_signals_majority_buy() {
        todo!()
    }

    #[test]
    fn combined_signals_with_mixed_signals_majority_sell() {
        todo!()
    }

    #[test]
    fn combined_signals_with_no_signals_returns_hold() {
        todo!()
    }

    #[test]
    fn combined_signals_with_equal_buy_and_sell_signals_returns_hold() {
        todo!()
    }
}
