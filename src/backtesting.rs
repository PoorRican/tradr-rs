use chrono::{DateTime};
use polars::prelude::*;

use crate::portfolio::{PerformanceMetrics, Portfolio, PortfolioArgs, PositionHandlers, TradeHandlers};
use crate::strategies::Strategy;
use crate::types::{ExecutedTrade, FutureTrade, Side};
use crate::utils;

pub struct BacktestingRunner<'a> {
    strategy: Strategy,
    portfolio_args: PortfolioArgs,
}

impl<'a> BacktestingRunner<'a> {
    pub fn new(strategy: Strategy, portfolio_args: PortfolioArgs) -> Self {
        BacktestingRunner {
            portfolio_args,
            strategy
        }
    }

    pub fn run(&mut self, candles: &DataFrame) -> Result<PerformanceMetrics, ()> {
        // process historical data
        self.strategy.bootstrap(candles);

        // initialize portfolio
        let start_time = candles.column("time").unwrap().datetime().unwrap().get(0).unwrap();
        let start_time = DateTime::from_timestamp_millis(start_time).unwrap().naive_utc();
        let mut portfolio = Portfolio::from_args(&self.portfolio_args, start_time);

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

                // remove rows which do not have a signal
                let mask = combined.column("signals").unwrap().i8().unwrap().not_equal(0);
                let combined = combined.filter(&mask).unwrap();

                // convert to vector of candles and signals
                let candles = utils::extract_candles_from_df(&combined).unwrap();
                let sides: Vec<Side> = utils::extract_side_from_df(&combined, "signals").unwrap();

                // trading logic
                for (candle, side) in candles.iter().zip(sides.iter()) {
                    let trade = match side {
                        Side::Buy => portfolio.generate_buy_opt(&candle),
                        Side::Sell => portfolio.generate_sell_opt(&candle)
                    };

                    // attempt trades
                    // TODO: simulate market conditions by adding randomness
                    if let Some(trade) = trade {
                        let executed = ExecutedTrade::with_future_trade("".to_string(), trade);
                        portfolio.add_executed_trade(executed);
                    }
                }

                println!("Trades: {:?}", portfolio.get_executed_trades());

                println!("Open Positions: {:?}", portfolio.get_open_positions().unwrap());

                // arbitrarily using a 3% risk-free rate
                Ok(portfolio.calculate_performance_metrics(0.03).unwrap())
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