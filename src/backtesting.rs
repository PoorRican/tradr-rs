use chrono::{DateTime};
use log::info;
use polars::prelude::*;
use crate::manager::{PositionManager, PositionManagerConfig, PositionManagerError, TradeDecision};
use crate::portfolio::{Portfolio, PortfolioArgs, PositionHandlers, TradeHandlers};
use crate::risk::{calculate_risk, RiskCalculationErrors};
use crate::strategies::Strategy;
use crate::types::{ExecutedTrade, FutureTrade, Side, Signal};
use crate::utils;


#[derive(Debug)]
pub enum BacktestingErrors {
    // The market data and historical data have different lengths
    CandleDataHasDifferentLengths,

    // The market data and historical data are not aligned by the same timestamp index
    CandleTimestampsNotAligned,

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

    /// Run the backtesting simulation
    pub fn run(&mut self, candles: &DataFrame, market_history: &DataFrame) -> Result<(), BacktestingErrors> {
        info!("Checking candle data and market data alignment");
        // ensure that the market data and historical data are sorted by timestamp
        let market_data_index = market_history.column("time").unwrap().datetime().unwrap();
        let historical_data_index = candles.column("time").unwrap().datetime().unwrap();
        if market_data_index.len() != historical_data_index.len() {
            return Err(BacktestingErrors::CandleDataHasDifferentLengths)
        }
        let index_alignment_mask: Vec<bool> = market_data_index.iter().zip(historical_data_index.iter()).map(|(a, b)| {
            a != b
        }).collect();
        if index_alignment_mask.iter().any(|&x| x) {
            return Err(BacktestingErrors::CandleTimestampsNotAligned)
        }

        // process historical data
        self.strategy.bootstrap(candles);

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
                    let risk = calculate_risk(&portfolio, &trimmed_candles, &trimmed_market)
                        .map_err(|e| {
                            info!("Error calculating risk: {:?}", e);
                            BacktestingErrors::RiskCalculationError(e)
                        })?;

                    let current_price = candle.close;

                    let decision = position_manager.make_decision(&mut portfolio, &risk, signal, current_price)
                        .map_err(|e| {
                            info!("Error making decision: {:?}", e);
                            BacktestingErrors::DecisionError(e)
                        })?;

                    let trade = match decision {
                        TradeDecision::ExecuteBuy(amount) => {
                            info!("Portfolio Risk Metrics: {:?}", risk);
                            FutureTrade::new_with_calculate_nominal(Side::Buy, current_price, amount, candle.time)
                        },
                        TradeDecision::ExecuteSell(amount, trade_ids) => {
                            info!("Portfolio Risk Metrics: {:?}", risk);
                            info!("Closing positions: {:?}", trade_ids);
                            FutureTrade::new_with_calculate_nominal(Side::Sell, current_price, amount, candle.time)
                        },
                        TradeDecision::DoNothing => continue,
                    };

                    // attempt trades
                    // TODO: simulate market conditions by adding randomness
                    let executed = ExecutedTrade::from_future_trade(candle.time.to_string(), trade);
                    portfolio.add_executed_trade(executed);
                }

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
}