use polars::prelude::*;
use crate::portfolio::{CapitalHandlers, Portfolio};

#[derive(Debug)]
pub struct PerformanceMetrics {
    total_return: f64,
    sharpe_ratio: f64,
    max_drawdown: f64,
    total_trades: usize,
}

impl Portfolio {
    pub fn calculate_performance_metrics(&self, risk_free_rate: f64) -> Result<PerformanceMetrics, PolarsError> {
        let df = &self.executed_trades;

        // Ensure the DataFrame is sorted by timestamp
        let df = df.sort(&["point"], SortMultipleOptions::new().with_order_descending(false))?;

        let total_return = self.calculate_total_return(&df)?;
        let total_trades = self.executed_trades.height();

        Ok(PerformanceMetrics {
            total_return,
            sharpe_ratio: self.calculate_sharpe_ratio(&df, risk_free_rate)?,
            max_drawdown: self.calculate_max_drawdown(&df)?,
            total_trades,
        })
    }

    fn calculate_total_return(&self, df: &DataFrame) -> Result<f64, PolarsError> {
        let initial_capital = self.capital_ts.get_last_value();
        let final_capital = self.get_capital();
        Ok((final_capital - initial_capital) / initial_capital)
    }

    fn calculate_sharpe_ratio(&self, df: &DataFrame, risk_free_rate: f64) -> Result<f64, PolarsError> {
        let returns = df.select(["cost", "side"])?
            .apply("cost", |s| {
                let costs = s.f64().unwrap();
                let sides = df.column("side").unwrap().utf8().unwrap();
                let returns = costs.into_iter().zip(sides.into_iter())
                    .map(|(cost, side)| if side == Some("Buy") { -cost } else { cost })
                    .collect::<Vec<f64>>();
                Series::new("returns", returns)
            })?;

        let mean_return = returns.mean().unwrap();
        let std_dev = returns.std(0).unwrap();

        Ok((mean_return - risk_free_rate) / std_dev)
    }

    fn calculate_max_drawdown(&self, df: &DataFrame) -> Result<f64, PolarsError> {
        let cumulative_returns = df.select(["cost", "side"])?
            .apply("cost", |s| {
                let costs = s.f64().unwrap();
                let sides = df.column("side").unwrap().utf8().unwrap();
                let mut cumulative = 1.0;
                let returns = costs.into_iter().zip(sides.into_iter())
                    .map(|(cost, side)| {
                        if side == Some("Buy") {
                            cumulative *= 1.0 - cost;
                        } else {
                            cumulative *= 1.0 + cost;
                        }
                        cumulative
                    })
                    .collect::<Vec<f64>>();
                Series::new("cumulative_returns", returns)
            })?;

        let peak = cumulative_returns.max().unwrap();
        let trough = cumulative_returns.min().unwrap();
        Ok((trough - peak) / peak)
    }
}