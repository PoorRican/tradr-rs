mod consensus;

use crate::indicators::{Indicator, IndicatorProcessingError};
pub use crate::strategies::consensus::Consensus;
use crate::types::Signal;
use polars::prelude::*;
use std::collections::HashMap;
use crate::processor::CandleProcessor;

#[derive(Debug)]
pub enum StrategyError {
    IndicatorError(IndicatorProcessingError),
}

/// A [`IndicatorContainer`] is a collection of [`Indicator`] objects.
type IndicatorContainer = Vec<Box<dyn Indicator>>;

/// A [`Strategy`] is a facade for interfacing with more than one [`Indicator`] objects.
///
/// A simple interface is provided for bootstrapping historical candle data, processing new candle data,
/// and generating a consensus [`Signal`] among all [`Indicator`] objects.
///
/// `Strategy::process_historical_candles` is used for bootstrapping the indicators with historical data.
/// `Strategy::process_new_candles` is used for ingesting new candle data and generating a consensus signal.
pub struct Strategy {
    pub indicators: IndicatorContainer,
    consensus: Consensus,
}

impl CandleProcessor for Strategy {
    type ErrorType = StrategyError;

    /// Process all historical data
    ///
    /// The internal state of all [`Indicator`] objects is updated with the historical data. Any existing
    /// data in the indicators is overwritten.
    ///
    /// This method is used upon initial load, or during backtesting.
    fn process_historical_candles(&mut self, candles: &DataFrame) -> Result<(), Self::ErrorType> {
        for indicator in self.indicators.iter_mut() {
            indicator.process_historical_candles(candles)
                .map_err(|x| StrategyError::IndicatorError(x))?;
        }
        Ok(())
    }

    /// Process a new candle and generate a consensus [`Signal`] among the [`Indicator`] objects.
    ///
    /// Internally, the dataframe is propagated to all internal indicators, and the resulting
    /// signals are gathered. A consensus is then reached between the signals, and returned.
    ///
    /// # Arguments
    /// * `row` - The new candle data to process
    fn process_new_candles(&mut self, candles: &DataFrame) -> Result<(), Self::ErrorType> {
        for indicator in self.indicators.iter_mut() {
            indicator.process_new_candles(candles)
                .map_err(|x| StrategyError::IndicatorError(x))?;
        }
        Ok(())
    }
}

impl Strategy {
    pub fn new(indicators: IndicatorContainer, consensus: Consensus) -> Self {
        Self {
            indicators,
            consensus,
        }
    }

    /// Get the last consensus [`Signal`] generated by the [`Indicator`] objects
    pub fn get_last_signal(&self) -> Signal {
        let signals = self
            .indicators
            .iter()
            .map(|x| x.get_last_signal().expect("No signal found"))
            .collect::<Vec<Signal>>();

        self.consensus.reduce(signals.into_iter())
    }

    pub fn get_all_graphs(&self) -> Result<Option<HashMap<&'static str, DataFrame>>, ()> {
        if self.indicators.is_empty() {
            Ok(None)
        } else {
            let graphs = self
                .indicators
                .iter()
                .map(|x| {
                    let graph = x.get_graph().unwrap();
                    (x.get_name(), graph.clone())
                })
                .collect::<HashMap<_, _>>();

            Ok(Some(graphs))
        }
    }

    /// Get a [`DataFrame`] of all signals generated by the [`Indicator`] objects.
    ///
    /// The column names correspond to the names of the [`Indicator`] objects. The `time` column is
    /// included.
    ///
    /// Returns `Ok(None)` if there are no indicators.
    ///
    /// # Errors
    ///
    /// Returns `()` if the indices of the signals are not aligned.
    ///
    /// Returns `()` if an indicator has no signals.
    ///
    /// Returns `()` if there was a [`PolarsError`] was caught
    fn get_all_signals(&self) -> Result<Option<DataFrame>, ()> {
        if self.indicators.is_empty() {
            return Ok(None);
        } else if self.indicators.len() == 1 {
            let indicator = &self.indicators[0];
            let df = indicator
                .get_signals()
                .unwrap()
                .clone()
                .rename("signals", indicator.get_name())
                .unwrap()
                .to_owned();
            return Ok(Some(df));
        }

        // ensure that indices for all indicators are the same
        let signals1 = self.indicators[0].get_signals();
        if signals1.is_none() {
            // TODO: state that the indicator has no signals
            return Err(());
        }
        // TODO: return err if any of the indicators are missing the "time" column
        let index1 = signals1.unwrap().column("time").unwrap();
        for indicator in self.indicators.iter() {
            let index = indicator.get_signals().unwrap().column("time").unwrap();
            if index1 != index {
                // TODO: state that the indices are not aligned
                return Err(());
            }
        }

        // combine all signals into a single DataFrame
        let mut df = df![
            "time" => index1.clone()
        ]
        .unwrap();

        for indicator in self.indicators.iter() {
            let signal_history = indicator
                .get_signals()
                .unwrap()
                .column("signals")
                .unwrap()
                .clone();
            df.replace_or_add(indicator.get_name(), signal_history)
                .unwrap();
        }

        Ok(Some(df))
    }

    /// Get all signal rows which are not [`Signal::Hold`].
    ///
    /// Reference [`Self::get_all_signals`] for what errors are returned.
    pub fn get_filtered_signals(&self) -> Result<Option<DataFrame>, ()> {
        if let Some(signals) = self.get_all_signals()? {
            // get all column names except for `time`
            let column_names = signals.get_column_names();
            let column_names = column_names
                .iter()
                .filter(|x| *x != &"time")
                .collect::<Vec<&&str>>();

            // for all columns, get a mask of the rows which are not 0 (ie: `Hold`)
            let mut combined_mask = None;
            column_names.iter().for_each(|x| {
                let mask = signals
                    .column(*x)
                    .unwrap()
                    .i8()
                    .unwrap()
                    .iter()
                    .map(|x| x != Some(Signal::Hold.into()))
                    .collect::<BooleanChunked>();
                if combined_mask.is_none() {
                    combined_mask = Some(mask);
                } else {
                    let _mask = combined_mask.as_ref().unwrap();
                    combined_mask = Some(_mask | &mask);
                }
            });
            let filtered = signals.filter(&combined_mask.unwrap()).unwrap();
            return Ok(Some(filtered));
        }
        Ok(None)
    }

    /// Use the [`Consensus`] field to combine all signals into a single "signal" column alongside of the "time" column.
    ///
    /// The rows returned are only those which are not [`Signal::Hold`].
    ///
    /// Reference [`Self::get_all_signals`] for what errors are returned.
    // TODO: untested
    pub fn get_combined_signals(&self) -> Result<Option<DataFrame>, ()> {
        if let Some(signals) = self.get_filtered_signals().unwrap() {
            let mut signals = signals;

            // Sum all signal columns
            let sum_expr: Vec<Expr> = signals
                .get_column_names()
                .into_iter()
                .filter(|&name| name != "time")
                .map(|name| col(name))
                .collect();

            let sum_signals = sum_expr.into_iter().reduce(|acc, x| acc + x).unwrap();

            signals = signals.lazy().with_column(sum_signals.alias("sum_signals")).collect().unwrap();

            // Apply consensus
            let consensus_expr = match self.consensus {
                Consensus::Unison => {
                    let n = self.indicators.len() as i8;
                    when(col("sum_signals").eq(lit(n)))
                        .then(lit(1i8))
                        .when(col("sum_signals").eq(lit(-n)))
                        .then(lit(-1i8))
                        .otherwise(lit(0i8))
                },
                Consensus::Majority => {
                    let n = (self.indicators.len() / 2) as i8;
                    when(col("sum_signals").gt(lit(n)))
                        .then(lit(1i8))
                        .when(col("sum_signals").lt(lit(-n)))
                        .then(lit(-1i8))
                        .otherwise(lit(0i8))
                },
            };

            signals = signals.lazy()
                .with_column(consensus_expr.cast(DataType::Int8).alias("signals"))
                .collect().unwrap();

            Ok(Some(signals.select(["time", "signals"]).unwrap()))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod strategy_tests {
    use super::*;
    use polars::prelude::*;
    use crate::indicators::BBands;

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
        ].unwrap()
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
