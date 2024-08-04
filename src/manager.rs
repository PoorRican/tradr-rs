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
use serde::{Deserialize, Serialize};
use crate::portfolio::{CapitalHandlers, Portfolio, PositionHandlers};
use crate::risk::{PortfolioRisk};
use crate::types::Trade;

#[derive(Error, Debug)]
pub enum PositionManagerError {
    #[error("Portfolio operation failed: {0}")]
    PortfolioError(String),
    #[error("Risk calculation failed: {0}")]
    RiskCalculationError(String),
    #[error("Invalid position size: {0}")]
    InvalidPositionSize(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionManagerConfig {
    // limits the total allowable capital allocated to open positions
    pub max_position_size: Decimal,

    // Sets the percentage loss at which to exit a position
    pub stop_loss_percentage: Decimal,

    // Sets the percentage gain at which to take profits
    pub take_profit_percentage: Decimal,

    // control the portfolio's volatility relative to the market
    pub max_beta: Decimal,

    // VaR limit ensures the potential loss doesn't exceed a certain threshold.
    pub var_limit: Decimal,

    // Defines the maximum allowable drawdown before halting trading
    // not used
    #[serde(default)]
    pub max_drawdown: Decimal,

    // ensure the risk-adjusted returns meet a certain threshold. Maintain balance between risk and return.
    pub min_sharpe_ratio: Decimal,

    // trigger profit-taking sells when it exceeds a certain threshold
    pub unrealized_pnl_limit: Decimal,
}

impl Default for PositionManagerConfig {
    fn default() -> Self {
        Self {
            max_position_size: dec!(100),
            stop_loss_percentage: dec!(0.05),
            take_profit_percentage: dec!(0.1),
            max_beta: dec!(1.4),
            var_limit: dec!(10),
            max_drawdown: dec!(0.2),  // unused
            min_sharpe_ratio: dec!(0.6),
            unrealized_pnl_limit: dec!(1.0),
        }
    }
}

pub struct PositionManager {
    config: PositionManagerConfig,
}

impl PositionManager {
    pub fn new(config: PositionManagerConfig) -> Self {
        Self {
            config,
        }
    }

    pub async fn update_config(&mut self, new_config: PositionManagerConfig) {
        self.config = new_config;
        info!("PositionManager configuration updated");
    }

    /// Verifies that the current drawdown hasn't exceeded the maximum allowed
    fn check_max_drawdown(&self) -> bool {
        // portfolio.current_drawdown() <= self.config.max_drawdown
        todo!("Portfolio doesn't have a drawdown method yet")
    }

    pub fn make_decision(&mut self, portfolio: &mut Portfolio, risk: &PortfolioRisk, signal: &Signal, current_price: Decimal) -> Result<TradeDecision, PositionManagerError> {
        // Check if we're within our risk tolerance
        if !self.is_within_risk_tolerance(&risk) {
            return Ok(TradeDecision::DoNothing)
        }

        match signal {
            Signal::Buy => self.process_buy_signal(portfolio, &risk, current_price),
            Signal::Sell => self.process_sell_signal(portfolio, &risk, current_price),
            Signal::Hold => Ok(TradeDecision::DoNothing),
        }
    }

    /// checks if the current risk profile is within tolerance using all the metrics
    fn is_within_risk_tolerance(&self, risk: &PortfolioRisk) -> bool {
        if risk.total_position_value == Decimal::ZERO {
            return true;
        }

        let max_position = risk.total_position_value <= self.config.max_position_size;
        let var_limit = risk.value_at_risk <= self.config.var_limit;
        let beta = risk.beta <= self.config.max_beta;
        let sharpe_ratio = risk.sharpe_ratio >= self.config.min_sharpe_ratio;

        if !max_position {
            warn!("Max position size exceeded: {}", risk.total_position_value);
        } else if !var_limit {
            warn!("VaR limit exceeded: {}", risk.value_at_risk);
        } else if !beta {
            warn!("Beta limit exceeded: {}", risk.beta);
        } else if !sharpe_ratio {
            warn!("Sharpe ratio below minimum: {}", risk.sharpe_ratio);
        }

        max_position && var_limit && beta && sharpe_ratio
    }

    /// calculates the available risk capacity based on the difference between the maximum allowed portfolio risk and current VaR.
    ///
    /// determines the maximum quantity that can be bought without exceeding this risk capacity.
    fn process_buy_signal(&self, portfolio: &Portfolio, risk: &PortfolioRisk, current_price: Decimal) -> Result<TradeDecision, PositionManagerError> {
        // Check if we're within our risk tolerance
        if !self.is_within_risk_tolerance(risk) {
            info!("Buy signal ignored: outside of risk tolerance");
            return Ok(TradeDecision::DoNothing);
        }

        let available_capital = portfolio.available_capital();
        if available_capital <= Decimal::ZERO {
            info!("Buy signal ignored: no available capital");
            if available_capital < Decimal::ZERO {
                warn!("Available capital is negative: {}", available_capital);
            }
            return Ok(TradeDecision::DoNothing);
        }

        // Calculate the available risk capacity
        let available_risk = self.config.var_limit - risk.value_at_risk;
        if available_risk <= Decimal::ZERO {
            info!("Buy signal ignored: no available risk capacity");
            return Ok(TradeDecision::DoNothing);
        }

        // Calculate the maximum quantity we can buy based on risk capacity and available capital
        let max_quantity_risk = available_risk / current_price;
        let max_quantity_capital = available_capital / current_price;
        let max_quantity = max_quantity_risk.min(max_quantity_capital);

        // Apply position size limits
        let position_limit = self.config.max_position_size / current_price;
        let buy_quantity = max_quantity.min(position_limit);

        if buy_quantity > Decimal::ZERO {
            info!("Executing buy for quantity: {}", buy_quantity);
            Ok(TradeDecision::ExecuteBuy(buy_quantity))
        } else {
            warn!("Calculated buy quantity is zero or negative");
            Ok(TradeDecision::DoNothing)
        }
    }

    /// checks if the unrealized PnL has reached the profit-taking threshold.
    ///
    /// checks if the VaR exceeds the limit and calculates how much to sell to bring the risk back within limits.
    fn process_sell_signal(&mut self, portfolio: &mut Portfolio, risk: &PortfolioRisk, current_price: Decimal) -> Result<TradeDecision, PositionManagerError> {
        let total_quantity = portfolio.total_open_quantity();

        if total_quantity == Decimal::ZERO {
            return Ok(TradeDecision::DoNothing);
        }

        // Check if we've reached the profit-taking threshold
        if risk.unrealized_pnl >= self.config.unrealized_pnl_limit {
            info!("Taking profit, attempting to sell total quantity: {}", total_quantity);
            let closed_trade_ids = portfolio.close_positions(total_quantity, current_price);
            return Ok(TradeDecision::ExecuteSell(total_quantity, closed_trade_ids));
        }

        // Check if we need to reduce risk
        if risk.value_at_risk > self.config.var_limit {
            let excess_risk = risk.value_at_risk - self.config.var_limit;
            let sell_quantity = (excess_risk / current_price).min(total_quantity);

            info!("Risk management sell, attempting to sell quantity: {}", sell_quantity);
            let closed_trade_ids = portfolio.close_positions(sell_quantity, current_price);
            return Ok(TradeDecision::ExecuteSell(sell_quantity, closed_trade_ids));
        }

        // Check stop-loss and take-profit for individual positions
        let open_positions = portfolio.get_open_positions()
            .clone();       // cloned to allow borrowing as mutable
        let mut total_sell_quantity = Decimal::ZERO;
        let mut closed_trade_ids = Vec::new();

        for (_, position) in open_positions {
            let stop_loss = position.entry_price * (Decimal::ONE - self.config.stop_loss_percentage);
            let take_profit = position.entry_price * (Decimal::ONE + self.config.take_profit_percentage);

            if current_price <= stop_loss || current_price >= take_profit {
                info!("Stop-loss or take-profit triggered for position: {:?}", position);
                let ids = portfolio.close_positions(position.quantity, current_price);
                total_sell_quantity += position.quantity;
                closed_trade_ids.extend(ids);
            }
        }

        if total_sell_quantity > Decimal::ZERO {
            return Ok(TradeDecision::ExecuteSell(total_sell_quantity, closed_trade_ids));
        }

        Ok(TradeDecision::DoNothing)
    }
}

pub enum TradeDecision {
    ExecuteBuy(Decimal),  // Quantity to buy
    ExecuteSell(Decimal, Vec<String>), // Quantity to sell
    DoNothing,
}