/// # NOTES
///
/// - Implementing dynamic risk limits that adjust based on market conditions or recent performance.
/// - Adding time-based factors, such as reducing risk tolerance near market close or during high-volatility periods.
/// - Incorporating correlation checks to ensure diversification when making buy decisions.
/// - Implementing a gradual position building/reduction strategy instead of all-or-nothing decisions.
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use thiserror::Error;
use log::{info, warn};
use crate::portfolio::{CapitalHandlers, Portfolio, PositionHandlers};
use crate::risk::{PortfolioRisk};
use crate::types::{Candle, Signal, Trade};

#[derive(Error, Debug)]
pub enum PositionManagerError {
    #[error("Portfolio operation failed: {0}")]
    PortfolioError(String),
    #[error("Risk calculation failed: {0}")]
    RiskCalculationError(String),
    #[error("Invalid position size: {0}")]
    InvalidPositionSize(String),
}

#[derive(Debug, Clone)]
struct PositionManagerConfig {
    // limits the size of any single position
    max_position_size: Decimal,

    // Sets the percentage loss at which to exit a position
    stop_loss_percentage: Decimal,

    // Sets the percentage gain at which to take profits
    take_profit_percentage: Decimal,

    // control the portfolio's volatility relative to the market
    max_beta: Decimal,

    // VaR limit ensures the potential loss doesn't exceed a certain threshold.
    var_limit: Decimal,

    // Defines the maximum allowable drawdown before halting trading
    max_drawdown: Decimal,

    // ensure the risk-adjusted returns meet a certain threshold. Maintain balance between risk and return.
    min_sharpe_ratio: Decimal,

    // trigger profit-taking sells when it exceeds a certain threshold
    unrealized_pnl_limit: Decimal,
}

pub struct PositionManager<'a> {
    config: PositionManagerConfig,
    portfolio: &'a Portfolio,
}

impl<'a> PositionManager<'a> {
    pub fn new(config: PositionManagerConfig, portfolio: &'a Portfolio) -> Self {
        Self {
            config,
            portfolio,
        }
    }

    pub async fn update_config(&mut self, new_config: PositionManagerConfig) {
        self.config = new_config;
        info!("PositionManager configuration updated");
    }
    /// Determines the appropriate size for a new position based on available capital and risk parameters
    fn calculate_position_size(&self) -> Decimal {
        let available_capital = self.portfolio.available_capital();
        Decimal::min(self.config.max_position_size, available_capital * dec!(.01)) // 1% of available capital
    }

    /// Verifies that the current drawdown hasn't exceeded the maximum allowed
    fn check_max_drawdown(&self) -> bool {
        // self.portfolio.current_drawdown() <= self.config.max_drawdown
        todo!("Portfolio doesn't have a drawdown method yet")
    }

    fn check_stop_loss_take_profit(&self, current_price: Decimal) -> Result<Option<TradeDecision>, PositionManagerError> {
        let open_positions = self.portfolio.get_open_positions().unwrap();

        for position in open_positions {
            let stop_loss = position.get_notional_value() * (Decimal::ONE - self.config.stop_loss_percentage);
            let take_profit = position.get_notional_value() * (Decimal::ONE + self.config.take_profit_percentage);

            if current_price <= stop_loss {
                info!("Stop-loss triggered for position: {:?}", position);
                return Ok(Some(TradeDecision::ExecuteSell(position.get_quantity())));
            }

            if current_price >= take_profit {
                info!("Take-profit triggered for position: {:?}", position);
                return Ok(Some(TradeDecision::ExecuteSell(position.get_quantity())));
            }
        }

        Ok(None)
    }

    pub fn make_decision(&self, risk: &PortfolioRisk, signal: Signal, current_price: Decimal) -> Result<TradeDecision, PositionManagerError> {
        // Check if we're within our risk tolerance
        if !self.is_within_risk_tolerance(&risk) {
            return Ok(TradeDecision::DoNothing)
        }

        match signal {
            Signal::Buy => self.process_buy_signal(&risk, current_price),
            Signal::Sell => self.process_sell_signal(&risk, current_price),
            Signal::Hold => Ok(TradeDecision::DoNothing)
        }
    }

    /// checks if the current risk profile is within tolerance using all the metrics
    fn is_within_risk_tolerance(&self, risk: &PortfolioRisk) -> bool {
        risk.total_position_value <= self.config.max_position_size
            && risk.value_at_risk <= self.config.var_limit
            && risk.beta <= self.config.max_beta
            && risk.sharpe_ratio >= self.config.min_sharpe_ratio
    }

    /// calculates the available risk capacity based on the difference between the maximum allowed portfolio risk and current VaR.
    ///
    /// determines the maximum quantity that can be bought without exceeding this risk capacity.
    fn process_buy_signal(&self, risk: &PortfolioRisk, current_price: Decimal) -> Result<TradeDecision, PositionManagerError> {
        let available_risk = self.config.var_limit - risk.value_at_risk;

        if available_risk <= dec!(0) {
            info!("No available risk capacity for buy signal");
            return Ok(TradeDecision::DoNothing);
        }

        let max_buy_quantity = available_risk / current_price;

        if max_buy_quantity > dec!(0.0) {
            info!("Executing buy for quantity: {}", max_buy_quantity);
            Ok(TradeDecision::ExecuteBuy(max_buy_quantity))
        } else {
            warn!("Calculated buy quantity is zero or negative");
            Ok(TradeDecision::DoNothing)
        }
    }

    /// checks if the unrealized PnL has reached the profit-taking threshold.
    ///
    /// checks if the VaR exceeds the limit and calculates how much to sell to bring the risk back within limits.
    fn process_sell_signal(&self, risk: &PortfolioRisk, current_price: Decimal) -> Result<TradeDecision, PositionManagerError> {
        let total_quantity = self.portfolio.get_open_positions().unwrap()
            .iter()
            .fold(dec!(0), |acc, trade| acc + trade.get_quantity());
        if risk.unrealized_pnl >= self.config.unrealized_pnl_limit {
            // TODO: this doesn't take into account individual trades

            info!("Taking profit, selling total quantity: {}", total_quantity);
            return Ok(TradeDecision::ExecuteSell(total_quantity));
        }

        if risk.value_at_risk > self.config.var_limit {
            let excess_risk = risk.value_at_risk - self.config.var_limit;
            let sell_quantity = excess_risk / Decimal::from(current_price);

            let final_sell_quantity = sell_quantity.min(total_quantity);

            info!("Risk management sell, quantity: {}", final_sell_quantity);
            return Ok(TradeDecision::ExecuteSell(final_sell_quantity));
        }

        Ok(TradeDecision::DoNothing)
    }
}

enum TradeDecision {
    ExecuteBuy(Decimal),  // Quantity to buy
    ExecuteSell(Decimal), // Quantity to sell
    DoNothing,
}