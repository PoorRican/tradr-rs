use chrono::{DateTime};
use polars::prelude::*;

use crate::portfolio::{PerformanceMetrics, Portfolio, PositionHandlers, TradeHandlers};
use crate::strategies::Strategy;
use crate::types::{ExecutedTrade, FutureTrade, Side};
use crate::utils;

pub struct BacktestingRunner<'a> {
    strategy: Strategy,
    portfolio: Portfolio,
    historical_data: &'a DataFrame,
}

impl<'a> BacktestingRunner<'a> {
    pub fn new(
        strategy: Strategy,
        init_assets: f64,
        init_capital: f64,
        candles: &'a DataFrame,
    ) -> Self {
        let start_time = candles.column("time").unwrap().datetime().unwrap().get(0).unwrap();
        let start_time = DateTime::from_timestamp_millis(start_time).unwrap().naive_utc();

        let portfolio = Portfolio::new(init_assets, init_capital, start_time);
        BacktestingRunner {
            portfolio,
            historical_data: candles,
            strategy
        }
    }

    pub fn run(&mut self) -> Result<PerformanceMetrics, ()> {
        // process historical data
        self.strategy.bootstrap(self.historical_data);

        if let Ok(signals) = self.strategy.get_combined_signals() {
            if let Some(signals) = signals {
                // join signals with historical data
                let combined = self.historical_data
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
                        Side::Buy => self.portfolio.generate_buy_opt(&candle),
                        Side::Sell => self.portfolio.generate_sell_opt(&candle)
                    };

                    // attempt trades
                    // TODO: simulate market conditions by adding randomness
                    if let Some(trade) = trade {
                        let executed = ExecutedTrade::with_future_trade("".to_string(), trade);
                        self.portfolio.add_executed_trade(executed);
                    }
                }

                println!("Trades: {:?}", self.portfolio.get_executed_trades());

                println!("Open Positions: {:?}", self.portfolio.get_open_positions().unwrap());

                // arbitrarily using a 3% risk-free rate
                Ok(self.portfolio.calculate_performance_metrics(0.03).unwrap())
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