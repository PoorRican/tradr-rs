use polars::prelude::*;
use crate::portfolio::{CapitalHandlers, Portfolio};
use std::ops::Mul;

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
            .lazy()
            .with_column(
                when(col("side").eq(lit("Buy")))
                    .then(col("cost").mul(lit(-1.0)))
                    .otherwise(col("cost"))
                    .alias("returns")
            )
            .collect()?;
        let returns = returns.column("returns")?.f64()?;

        let mean_return = returns.mean().unwrap();
        let std_dev = returns.std(0).unwrap();

        Ok((mean_return - risk_free_rate) / std_dev)
    }

    fn calculate_max_drawdown(&self, df: &DataFrame) -> Result<f64, PolarsError> {
        let mut cumulative_returns = df.select(["cost", "side"])?;
        let cumulative_returns = cumulative_returns
            .apply("cost", |s| {
                let costs = s.f64().unwrap();
                let sides = df.column("side").unwrap().str().unwrap();
                let mut cumulative = 1.0;
                let returns = costs.into_iter().zip(sides.into_iter())
                    .map(|(cost, side)| {
                        if let Some(c) = cost {
                            if side == Some("Buy") {
                                cumulative *= 1.0 - c;
                            } else {
                                cumulative *= 1.0 + c;
                            }
                        }
                        cumulative
                    })
                    .collect::<Vec<f64>>();
                Series::new("cumulative_returns", returns)
            })?;

        let peak: f64 = cumulative_returns.column("cumulative_returns").unwrap().max().unwrap().unwrap();
        let trough: f64 = cumulative_returns.column("cumulative_returns").unwrap().min().unwrap().unwrap();
        Ok((trough - peak.clone()) / peak)
    }
}