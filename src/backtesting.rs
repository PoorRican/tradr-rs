use std::path::Path;
use chrono::{DateTime};
use log::info;
use polars::prelude::*;
use serde::Deserialize;
use crate::manager::{PositionManager, PositionManagerConfig, PositionManagerError, TradeDecision};
use crate::portfolio::{Portfolio, PortfolioArgs, PositionHandlers, TradeHandlers};
use crate::processor::CandleProcessor;
use crate::risk::{calculate_risk, RiskCalculationErrors};
use crate::strategies::Strategy;
use crate::types::{ExecutedTrade, FutureTrade, Side, Signal};
use crate::utils;
use crate::utils::{AlignmentError, check_candle_alignment};

#[derive(Deserialize, Debug)]
pub struct BacktestingConfig {
    portfolio: PortfolioArgs,
    risk: PositionManagerConfig,
}

#[derive(Debug)]
pub enum BacktestingErrors {
    AlignmentError(AlignmentError),

    RiskCalculationError(RiskCalculationErrors),
    DecisionError(PositionManagerError),
}

pub struct BacktestingRunner {
    strategy: Strategy,
    portfolio_args: PortfolioArgs,
    manager_config: PositionManagerConfig,
}

impl BacktestingRunner {
    pub fn new(strategy: Strategy, portfolio_args: PortfolioArgs, manager_config: PositionManagerConfig) -> Self {
        BacktestingRunner {
            portfolio_args,
            strategy,
            manager_config
        }
    }

    pub fn from_config(config_path: &str, strategy: Strategy) -> Self {
        let config_str = std::fs::read_to_string(config_path).unwrap();
        let config: BacktestingConfig = toml::from_str(&config_str).unwrap();

        BacktestingRunner {
            portfolio_args: config.portfolio,
            strategy,
            manager_config: config.risk,
        }
    }

    /// Run the backtesting simulation
    pub fn run(&mut self, candles: &DataFrame, market_history: &DataFrame) -> Result<(), BacktestingErrors> {
        info!("Checking candle data and market data alignment");
        // ensure that the market data and historical data are sorted by timestamp
        let _ = check_candle_alignment(&candles, &market_history)
            .map_err(|e| BacktestingErrors::AlignmentError(e));

        // process historical data
        self.strategy.process_historical_candles(candles).unwrap();

        // initialize portfolio
        let start_time = candles.column("time").unwrap().datetime().unwrap().get(0).unwrap();
        let start_time = DateTime::from_timestamp_millis(start_time).unwrap().naive_utc();
        let mut portfolio = Portfolio::from_args(&self.portfolio_args, start_time);

        // initialize position manager
        let mut position_manager = PositionManager::new(self.manager_config.clone());

        if let Ok(signals) = self.strategy.get_combined_signals() {
            if let Some(signals) = signals {
                // join signals with historical data
                let combined = candles
                    .clone()
                    .lazy()
                    .join(
                        signals.lazy(),
                        [col("time")],
                        [col("time")],
                        JoinArgs::new(JoinType::Left),
                    )
                    .collect().unwrap();

                // ensure that columns are correct
                assert_eq!(combined.get_column_names(), &["time", "open", "high", "low", "close", "volume", "signals"]);

                // convert to vector of candles and signals
                let candles_vec = utils::extract_candles_from_df(&combined).unwrap();
                let sides: Vec<Signal> = utils::extract_signals_from_df(&combined, "signals").unwrap();

                let rows = candles_vec.iter().zip(sides.iter());

                // trading logic
                for (candle, signal) in rows {
                    // trim candle data
                    let trimmed_candles = candles.clone()
                        .lazy()
                        .filter(col("time").lt(lit(candle.time)))
                        .tail(100)
                        .collect()
                        .unwrap();
                    if trimmed_candles.height() == 0 {
                        continue;
                    }
                    let trimmed_candles = utils::extract_candles_from_df(&trimmed_candles).unwrap();

                    // trim market data
                    let trimmed_market = market_history.clone()
                        .lazy()
                        .filter(col("time").lt(lit(candle.time)))
                        .tail(100)
                        .collect()
                        .unwrap();
                    let trimmed_market = utils::extract_candles_from_df(&trimmed_market).unwrap();

                    // calculate current portfolio risk metrics
                    let risk = calculate_risk(&portfolio, &trimmed_market, &trimmed_candles)
                        .map_err(|e| {
                            info!("Error calculating risk: {:?}", e);
                            BacktestingErrors::RiskCalculationError(e)
                        })?;

                    let current_price = candle.close;

                    // make decision based on risk, signals and current market conditions
                    let decision = position_manager.make_decision(&mut portfolio, &risk, signal, current_price)
                        .map_err(|e| {
                            info!("Error making decision: {:?}", e);
                            BacktestingErrors::DecisionError(e)
                        })?;

                    let trade = match decision {
                        TradeDecision::ExecuteBuy(quantity) => {
                            FutureTrade::new(Side::Buy, current_price, quantity, candle.time)
                        },
                        TradeDecision::ExecuteSell(quantity, trade_ids) => {
                            info!("Closing positions: {:?}", trade_ids);
                            FutureTrade::new(Side::Sell, current_price, quantity, candle.time)
                        },
                        TradeDecision::DoNothing => continue,
                    };

                    // attempt trades
                    // TODO: simulate market conditions by adding randomness
                    let executed = ExecutedTrade::from_future_trade(candle.time.to_string(), trade);
                    portfolio.add_executed_trade(executed);
                }

                // print basic statistics
                print_portfolio(&portfolio);

                Ok(())
            } else {
                // TODO: return err for no indicators
                todo!()
            }
        } else {
            // TODO: return err for could not combine signals
            todo!()
        }
    }

    pub fn get_strategy(&self) -> &Strategy {
        &self.strategy
    }

    pub fn get_strategy_as_mut(&mut self) -> &mut Strategy {
        &mut self.strategy
    }

    /// Write all indicator graphs to a specific dir
    ///
    /// # Arguments
    /// * `dir` - The path to save the indicator graph files
    pub fn save_indicator_data(&mut self, dir: &str) -> Result<(), PolarsError> {
        let dir_path = Path::new(dir);

        for indicator in self.strategy.indicators.iter_mut() {
            let mut indicator = indicator.as_mut();
            let file_name = format!("{}_graph.csv", indicator.get_name());
            let path = dir_path.join(file_name);
            let path = path.to_str().unwrap();
            indicator.save_graph_as_csv(path)?;
        }
        Ok(())
    }
}

fn print_portfolio(portfolio: &Portfolio) {
    info!("Number of open positions: {}", portfolio.get_open_positions().len());
    info!("Total open quantity: {}", portfolio.total_open_quantity());
    info!("Total open value: {}", portfolio.total_position_value());
    info!("Total positions: {}", portfolio.get_executed_trades().len());
}
