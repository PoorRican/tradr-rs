use polars::prelude::*;
use crate::portfolio::{CapitalHandlers, Portfolio};
use std::ops::Mul;
use crate::types::Side;

#[derive(Debug)]
pub struct PerformanceMetrics {
    total_return: f64,
    sharpe_ratio: f64,
    max_drawdown: f64,
    total_trades: usize,
}

impl Portfolio {
    pub fn calculate_performance_metrics(&self, risk_free_rate: f64) -> Result<PerformanceMetrics, PolarsError> {
        todo!()
    }

    // TODO: bad implementation
    fn calculate_total_return(&self, df: &DataFrame) -> Result<f64, PolarsError> {
        todo!()
    }

    // TODO: bad implementation
    fn calculate_sharpe_ratio(&self, df: &DataFrame, risk_free_rate: f64) -> Result<f64, PolarsError> {
        todo!()
    }

    // TODO: bad implementation
    fn calculate_max_drawdown(&self, df: &DataFrame) -> Result<f64, PolarsError> {
        let costs = df.column("cost")?.f64().unwrap();
        let sides = df.column("side").unwrap().i8().unwrap();
        let mut cumulative = 1.0;
        let returns = costs.into_iter().zip(sides.into_iter())
            .map(|(cost, side)| {
                if let Some(c) = cost {
                    if side == Some(Side::Buy.into()) {
                        cumulative *= 1.0 - c;
                    } else {
                        cumulative *= 1.0 + c;
                    }
                }
                cumulative
            })
            .collect::<Vec<f64>>();
        let returns = Series::new("cumulative_returns", returns);

        let peak: f64 = returns.max().unwrap().unwrap();
        let trough: f64 = returns.min().unwrap().unwrap();
        Ok((trough - peak.clone()) / peak)
    }
}