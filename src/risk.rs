/// Functions for calculating risk metrics for a portfolio
///
/// The primary function is [`calculate_risk`], which accepts a [`Portfolio`] and market data as input and returns a [`PortfolioRisk`] struct.
use rust_decimal::{Decimal, MathematicalOps};
use rust_decimal_macros::dec;
use crate::portfolio::{Portfolio, PositionHandlers};
use crate::types::{Candle, Trade};

pub enum RiskCalculationErrors {
    /// The market data and historical data are not aligned by timestamp
    CandleDataNotAligned
}

/// Calculate risk metrics for a portfolio against market data, and historical data for the asset.
///
/// # Arguments
/// - `portfolio` - The portfolio to calculate risk metrics for
/// - `market_data` - Historical market data for the asset
/// - `historical_data` - Historical data for the asset
///
/// # Returns
///
/// A [`PortfolioRisk`] struct containing the calculated risk metrics
///
/// # Errors
///
/// - [`RiskCalculationErrors::CandleDataNotAligned`] - The market data and historical data are not aligned by timestamp
pub fn calculate_risk(portfolio: &Portfolio, market_data: &[Candle], historical_data: &[Candle]) -> Result<PortfolioRisk, RiskCalculationErrors> {
    // ensure that the market data and historical data are sorted by timestamp
    let market_data_index = market_data.iter().map(|candle| candle.time).collect::<Vec<_>>();
    let historical_data_index = historical_data.iter().map(|candle| candle.time).collect::<Vec<_>>();

    if market_data_index != historical_data_index {
        return Err(RiskCalculationErrors::CandleDataNotAligned)
    }

    let current_price = get_current_price(historical_data);
    let (total_position_value, average_entry_price, unrealized_pnl) = calculate_position_metrics(portfolio, current_price);
    let returns = calculate_returns(historical_data);

    let value_at_risk = calculate_value_at_risk(&returns, total_position_value);
    let beta = calculate_beta(market_data, &returns);
    let sharpe_ratio = calculate_sharpe_ratio(&returns);

    Ok(PortfolioRisk {
        total_position_value,
        average_entry_price,
        unrealized_pnl,
        value_at_risk,
        beta,
        sharpe_ratio,
    })
}

/// Calculate total position value, average entry price, and unrealized P&L for a portfolio
fn calculate_position_metrics(portfolio: &Portfolio, current_price: Decimal) -> (Decimal, Decimal, Decimal) {
    let mut total_position_value = dec!(0);
    let mut total_cost = dec!(0);
    let mut total_quantity = dec!(0);

    for trade in portfolio.get_open_positions().unwrap().iter() {
        let quantity = trade.get_quantity();
        let cost = trade.get_cost();

        total_position_value += quantity * current_price;
        total_cost += cost;
        total_quantity += quantity;
    }

    let average_entry_price = if total_quantity.is_zero() {
        dec!(0)
    } else {
        total_cost / total_quantity
    };

    let unrealized_pnl = total_position_value - total_cost;

    (total_position_value, average_entry_price, unrealized_pnl)
}

/// Quantifies the level of financial risk over a specific time frame with a given confidence interval.
///
/// Defaults to a 95% confidence interval, which means that there is a 5% chance that the portfolio
/// will lose more than the VaR estimate over the defined period.
fn calculate_value_at_risk(returns: &[Decimal], total_position_value: Decimal) -> Decimal {
    let mut sorted_returns = returns.to_vec();
    sorted_returns.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let var_index = (sorted_returns.len() as f64 * 0.05) as usize;
    let var_95 = sorted_returns.get(var_index).cloned().unwrap_or_default();

    total_position_value * var_95
}

/// Measure the volatility of an asset compared against the market
fn calculate_beta(market_data: &[Candle], asset_returns: &[Decimal]) -> Decimal {
    let market_returns = calculate_returns(market_data);

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

/// Measure additional return for the volatility endured for holding a riskier asset
///
/// This does not account for the risk-free rate, which is a common simplification for algo trading
/// because it should be negligible for short-term trading.
fn calculate_sharpe_ratio(returns: &[Decimal]) -> Decimal {
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

fn get_current_price(historical_data: &[Candle]) -> Decimal {
    historical_data.last().unwrap().close
}

fn calculate_returns(candles: &[Candle]) -> Vec<Decimal> {
    candles.windows(2)
        .map(|window| {
            let [previous, current] = window else { unreachable!() };
            (current.close - previous.close) / previous.close
        })
        .collect()
}


/// Risk metrics for a portfolio
///
/// # Measurements
///
/// These are the more complex risk metrics returned.
///
/// ## Value at Risk (VaR)
///
/// Quantifies the level of financial risk over a specific time frame with a given confidence interval.
///
/// For example, a 95% confidence interval, means that there is a 5% chance that the portfolio will
/// lose more than the VaR estimate over the defined period.
///
///
/// ## Beta
///
/// Measures the correlation and volatility between the asset and the market.
///
/// ### Interpretation
///
/// - 1: The asset moves in line with the market.
/// - > 1: The asset is more volatile than the market.
/// - < 1: The asset is less volatile than the market.
/// - = 0: The asset's returns have no correlation with the market.
/// - Negative: The asset tends to move in the opposite direction of the market.
///
///
/// ## Sharpe Ratio
///
/// Measures the additional return for the volatility endured for holding a riskier asset.
///
/// ### Interpretation
///
/// - > 1: The asset is generating a return above the risk-free rate for the volatility endured.
/// - < 1: The asset is generating a return below the risk-free rate for the volatility endured.
///
/// ### Uses
///
/// - **Assessing Strategies:** a strategy with a higher Sharpe ratio is generally considered
///   better as it provides more return for the same amount of risk.
/// - **Risk Management:** Aids in understanding if the returns of a strategy justify the risk it's
///   taking. Crucial for maintaining a balanced risk profile in algorithmic trading.
/// - **Performance Metric:** Evaluates the performance of algorithms over time.
/// - **Strategy Optimization:** Can be used as an optimization target, adjusting trading parameters to
///   maximize the Sharpe ratio, aiming for the best risk-adjusted returns.
/// - **Capital Allocation:** In a system with multiple trading strategies, the Sharpe ratio can guide capital
///   allocation/distribution. Strategies with higher Sharpe ratios might receive more capital.
/// - **Robustness Check:** A consistently high Sharpe ratio across different market conditions can indicate
/// a robust trading strategy.
pub struct PortfolioRisk {
    total_position_value: Decimal,
    average_entry_price: Decimal,
    unrealized_pnl: Decimal,
    value_at_risk: Decimal,
    beta: Decimal,
    sharpe_ratio: Decimal,
}