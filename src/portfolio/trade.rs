use crate::portfolio::assets::AssetHandlers;
use crate::portfolio::capital::CapitalHandlers;
use crate::portfolio::position::PositionHandlers;
use crate::portfolio::Portfolio;
use crate::types::{Candle, ExecutedTrade, FailedTrade, FutureTrade, Side, Trade};
use chrono::NaiveDateTime;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;

/// Interface methods for storing and retrieving trades, and determining when to trade
pub trait TradeHandlers: PositionHandlers + AssetHandlers + CapitalHandlers {
    fn get_executed_trades(&self) -> &HashMap<NaiveDateTime, ExecutedTrade>;
    fn add_failed_trade(&mut self, trade: FailedTrade);
    fn add_executed_trade(&mut self, trade: ExecutedTrade);

    #[deprecated(note = "Responsibility is moving to crate::PositionManager")]
    fn generate_sell_opt(&self, candle: &Candle) -> Option<FutureTrade>;
    #[deprecated(note = "Responsibility is moving to crate::PositionManager")]
    fn generate_buy_opt(&self, candle: &Candle) -> Option<FutureTrade>;
    #[deprecated(note = "Responsibility is moving to crate::PositionManager")]
    fn get_buy_cost(&self) -> Decimal;
    fn get_last_trade(&self) -> Option<&ExecutedTrade>;
    #[deprecated(note = "Responsibility is moving to crate::PositionManager")]
    fn able_to_buy(&self) -> bool;
}

impl TradeHandlers for Portfolio {
    /// Get the executed trades
    fn get_executed_trades(&self) -> &HashMap<NaiveDateTime, ExecutedTrade> {
        &self.executed_trades
    }

    /// Add a failed trade to the portfolio
    ///
    /// Storing "failed trades" is only intended for debugging and backtesting purposes.
    ///
    /// # Arguments
    /// * `trade` - The failed trade to add
    fn add_failed_trade(&mut self, trade: FailedTrade) {
        self.failed_trades.push(trade)
    }

    /// Add an executed trade to the portfolio
    ///
    /// Adding an executed trade will update the capital and assets of the portfolio.
    ///
    /// # Arguments
    /// * `trade` - The executed trade to add
    fn add_executed_trade(&mut self, trade: ExecutedTrade) {
        if trade.get_side() == Side::Buy {
            self.decrease_capital(trade.get_notional_value(), *trade.get_timestamp());
            self.increase_assets(trade.get_quantity(), *trade.get_timestamp());
            self.add_open_position(&trade);
        } else {
            self.increase_capital(trade.get_notional_value(), *trade.get_timestamp());
            self.decrease_assets(trade.get_quantity(), *trade.get_timestamp());
            self.close_positions(trade.get_quantity(), trade.get_notional_value());
        }
        self.executed_trades.insert(*trade.get_timestamp(), trade);
    }

    fn generate_sell_opt(&self, candle: &Candle) -> Option<FutureTrade> {
        todo!()
    }

    fn generate_buy_opt(&self, candle: &Candle) -> Option<FutureTrade> {
        if !self.able_to_buy() {
            return None;
        }
        let rate = calculate_buy_rate(candle);
        let cost = self.get_buy_cost();
        Some(FutureTrade::new(Side::Buy, rate, cost, candle.time))
    }

    /// The amount of capital to use for a single buy trade
    ///
    /// This number is determined by the amount of capital available and the number of open positions.
    fn get_buy_cost(&self) -> Decimal {
        todo!()
    }

    /// Get the most recent trade
    ///
    /// # Returns
    /// * `Some` - The most recent trade
    /// * `None` - If there are no trades
    fn get_last_trade(&self) -> Option<&ExecutedTrade> {
        if self.executed_trades.len() > 0 {
            let last_ts = self.executed_trades.keys().max().unwrap();
            self.executed_trades.get(last_ts)
        } else {
            None
        }
    }

    /// Get a boolean indicating whether or not the portfolio is able to buy.
    ///
    /// # Conditions
    /// Buys are allowed when:
    /// * There are not too many open positions
    /// * If the last trade was a sell
    /// * If the timeout has expired since the last trade
    ///
    /// Buys are prevented when:
    /// * There are too many open positions
    /// * The last trade was a buy and the timeout has not expired
    ///
    /// # Returns
    /// `true` if the portfolio is able to buy, `false` otherwise
    ///
    fn able_to_buy(&self) -> bool {
        todo!()
    }
}

fn calculate_buy_rate(candle: &Candle) -> Decimal {
    ((candle.close * dec!(2.0)) + candle.high + candle.open) / dec!(4.0)
}

#[cfg(test)]
mod tests {
    use crate::portfolio::{AssetHandlers, CapitalHandlers, Portfolio, TradeHandlers};
    use crate::types::{ExecutedTrade, FailedTrade, ReasonCode, Side, Trade};
    use chrono::{Duration, NaiveDateTime, Utc};
    use rust_decimal_macros::dec;

    /// Test that a failed trade is correctly added to the portfolio storage.
    /// Since the failed storage is only for debugging and backtestesting, no other checks
    /// are necessary.
    #[test]
    fn test_add_failed_trade() {
        let mut portfolio = Portfolio::new(dec!(100.0), dec!(100.0), None);
        assert!(portfolio.failed_trades.is_empty());

        // add a failed buy
        let trade = FailedTrade::new(
            ReasonCode::Unknown,
            Side::Buy,
            dec!(100.0),
            dec!(1.0),
            NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap(),
        );
        portfolio.add_failed_trade(trade);
        assert_eq!(portfolio.failed_trades.len(), 1);

        // add a failed sell
        let trade = FailedTrade::new(
            ReasonCode::Unknown,
            Side::Sell,
            dec!(100.0),
            dec!(1.0),
            NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap(),
        );
        portfolio.add_failed_trade(trade);
        assert_eq!(portfolio.failed_trades.len(), 2);
    }

    /// Test that an executed trade is correctly added to the portfolio storage
    /// and that the capital and assets are updated appropriately
    #[test]
    fn test_add_executed_trade() {
        let mut portfolio = Portfolio::new(dec!(200.0), dec!(200.0), None);

        // handle a buy
        let trade = ExecutedTrade::with_calculated_notional(
            "id".to_string(),
            Side::Buy,
            dec!(100.0),
            dec!(1.0),
            NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap(),
        );
        assert!(portfolio.executed_trades.is_empty());
        assert_eq!(portfolio.available_capital(), dec!(200.0));
        assert_eq!(portfolio.get_assets(), dec!(200.0));

        portfolio.add_executed_trade(trade);
        assert_eq!(portfolio.executed_trades.len(), 1);
        assert_eq!(portfolio.open_positions.len(), 1);

        // check that capital and assets are updated
        assert_eq!(portfolio.available_capital(), dec!(100.0));
        assert_eq!(portfolio.get_assets(), dec!(201.0));

        // handle a sell
        let trade = ExecutedTrade::with_calculated_notional(
            "id".to_string(),
            Side::Sell,
            dec!(100.0),
            dec!(1.0),
            (Utc::now() + Duration::seconds(1)).naive_utc(),
        );
        portfolio.add_executed_trade(trade);
        assert_eq!(portfolio.executed_trades.len(), 2);

        // check that capital and assets are updated
        assert_eq!(portfolio.available_capital(), dec!(200.0));
        assert_eq!(portfolio.get_assets(), dec!(200.0));
        assert_eq!(portfolio.open_positions.len(), 0);
    }

    #[test]
    fn test_last_trade() {
        let mut portfolio = Portfolio::new(dec!(200.0), dec!(200.0), None);
        assert!(portfolio.get_last_trade().is_none());

        let trade = ExecutedTrade::with_calculated_notional(
            "id".to_string(),
            Side::Buy,
            dec!(100.0),
            dec!(1.0),
            Utc::now().naive_utc(),
        );
        portfolio.add_executed_trade(trade);

        let last_trade = portfolio.get_last_trade();
        assert!(portfolio.get_last_trade().is_some());

        // append another trade and assert that the last trade is this new trade
        let id = "id".to_string();
        let side = Side::Sell;
        let price = dec!(121.0);
        let quantity = dec!(1.0);
        let time = Utc::now().naive_utc();

        let trade =
            ExecutedTrade::with_calculated_notional("id".to_string(), side, price, quantity, time);
        portfolio.add_executed_trade(trade);

        let last_trade = portfolio.get_last_trade().unwrap();
        assert_eq!(last_trade.get_order_id(), &id);
        assert_eq!(last_trade.get_side(), side);
        assert_eq!(last_trade.get_price(), price);
        assert_eq!(last_trade.get_quantity(), quantity);
        assert_eq!(
            last_trade.get_timestamp().timestamp_millis(),
            time.timestamp_millis()
        );
    }
}
