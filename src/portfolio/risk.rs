use rust_decimal::{Decimal, MathematicalOps};
use rust_decimal_macros::dec;
use crate::portfolio::{Portfolio, PositionHandlers};
use crate::types::{Candle, Trade};

impl Portfolio {
    pub fn calculate_risk(&self, market_data: &[Candle], historical_data: &[Candle]) -> PortfolioRisk {
        let current_price = self.get_current_price(historical_data);
        let total_position_value = self.calculate_total_position_value(current_price);
        let average_entry_price = self.calculate_average_entry_price();
        let unrealized_pnl = self.calculate_unrealized_pnl(current_price);
        let value_at_risk = self.calculate_value_at_risk(historical_data);
        let beta = self.calculate_beta(market_data, historical_data);
        let sharpe_ratio = self.calculate_sharpe_ratio(historical_data);

        PortfolioRisk {
            total_position_value,
            average_entry_price,
            unrealized_pnl,
            value_at_risk,
            beta,
            sharpe_ratio,
        }
    }

    /// Sums the current value of all open positions based on the latest price
    fn calculate_total_position_value(&self, current_price: Decimal) -> Decimal {
        self.get_open_positions().unwrap().iter().fold(dec!(0), |acc, trade| {
            acc + trade.get_quantity() * current_price
        })
    }

    /// Calculates the weighted average price at which the open positions were entered
    fn calculate_average_entry_price(&self) -> Decimal {
        let (total_cost, total_quantity) = self.get_open_positions().unwrap().iter().fold((dec!(0), dec!(0)), |(cost, qty), trade| {
            (cost + trade.get_cost(),
             qty + trade.get_quantity())
        });
        if total_quantity.is_zero() {
            dec!(0)
        } else {
            total_cost / total_quantity
        }
    }

    /// Computes the current unrealized profit or loss of all open positions
    fn calculate_unrealized_pnl(&self, current_price: Decimal) -> Decimal {
        self.get_open_positions().unwrap().iter().fold(dec!(0), |acc, trade| {
            let position_value = Decimal::from(trade.get_quantity() * current_price);
            let cost_basis = Decimal::from(trade.get_cost());
            acc + (position_value - cost_basis)
        })
    }

    /// Estimates the potential loss in value of the portfolio over a defined period for a given confidence interval.
    ///
    /// This implementation uses a 95% confidence interval.
    fn calculate_value_at_risk(&self, historical_data: &[Candle]) -> Decimal {
        let returns: Vec<Decimal> = historical_data.windows(2)
            .map(|window| {
                let prev = window[0].close;
                let current = window[1].close;
                (current - prev) / prev
            })
            .collect();

        let sorted_returns: Vec<Decimal> = {
            let mut sorted = returns.clone();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            sorted
        };

        let var_index = (sorted_returns.len() as f64 * 0.05) as usize;
        let var_95 = sorted_returns.get(var_index).cloned().unwrap_or_default();

        self.calculate_total_position_value(historical_data.last().unwrap().close) * var_95
    }

    /// Measures the volatility of the asset relative to the market.
    ///
    /// A beta of 1 indicates that the asset moves with the market, less than 1 is less volatile,
    /// and greater than 1 is more volatile.
    ///
    /// # Arguments
    ///
    /// * `market_data` - The market data to compare against (ie: S&P 500, BTC)
    /// * `historical_data` - The portfolio's asset historical data
    fn calculate_beta(&self, market_data: &[Candle], historical_data: &[Candle]) -> Decimal {
        // Assuming the last candle in historical_data represents the market
        let market_returns: Vec<Decimal> = calculate_returns(market_data);

        let asset_returns: Vec<Decimal> = calculate_returns(historical_data);

        let (sum_xy, sum_x, sum_y, sum_x_squared) = market_returns.iter().zip(asset_returns.iter())
            .fold((dec!(0), dec!(0), dec!(0), dec!(0)), |acc, (&x, &y)| {
                (acc.0 + x * y, acc.1 + x, acc.2 + y, acc.3 + x * x)
            });

        let n = Decimal::from(market_returns.len());
        let numerator = n * sum_xy - sum_x * sum_y;
        let denominator = n * sum_x_squared - sum_x * sum_x;

        if denominator.is_zero() {
            dec!(0)
        } else {
            numerator / denominator
        }
    }

    /// Calculates the risk-adjusted return of the portfolio.
    ///
    /// A higher Sharpe ratio indicates better risk-adjusted performance.
    fn calculate_sharpe_ratio(&self, historical_data: &[Candle]) -> Decimal {
        let returns: Vec<Decimal> = calculate_returns(historical_data);

        let mean_return = returns.iter().sum::<Decimal>() / Decimal::from(returns.len());
        let variance = returns.iter()
            .map(|&r| (r - mean_return) * (r - mean_return))
            .sum::<Decimal>() / Decimal::from(returns.len() - 1);
        let std_dev = variance.sqrt().unwrap();

        if std_dev.is_zero() {
            dec!(0)
        } else {
            mean_return / std_dev
        }
    }

    fn get_current_price(&self, historical_data: &[Candle]) -> Decimal {
        Decimal::from(historical_data.last().unwrap().close)
    }}

fn calculate_returns(candles: &[Candle]) -> Vec<Decimal> {
    candles.windows(2)
        .map(|window| {
            let [previous, current] = window else { unreachable!() };
            let r = (current.close - previous.close) / previous.close;
            Decimal::from(r)
        })
        .collect()
}

pub struct PortfolioRisk {
    total_position_value: Decimal,
    average_entry_price: Decimal,
    unrealized_pnl: Decimal,
    value_at_risk: Decimal,
    beta: Decimal,
    sharpe_ratio: Decimal,
}