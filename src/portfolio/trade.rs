use chrono::{NaiveDateTime, Utc};
use polars::prelude::DataFrame;
use crate::portfolio::assets::AssetHandlers;
use crate::portfolio::capital::CapitalHandlers;
use crate::portfolio::Portfolio;
use crate::portfolio::position::PositionHandlers;
use crate::traits::AsDataFrame;
use crate::types::{
    Side,
    ExecutedTrade, FailedTrade, FutureTrade, Trade
};

/// Interface methods for storing trades
pub trait TradeHandlers: PositionHandlers + AssetHandlers + CapitalHandlers {
    fn get_executed_trades(&self) -> &DataFrame;
    fn add_failed_trade(&mut self, trade: FailedTrade);
    fn add_executed_trade(&mut self, trade: ExecutedTrade);
    fn is_rate_profitable(&self, rate: f64) -> Option<FutureTrade>;
}

impl TradeHandlers for Portfolio {
    /// Get the executed trades
    fn get_executed_trades(&self) -> &DataFrame {
        &self.executed_trades
    }
    
    /// Add a failed trade to the portfolio
    ///
    /// Storing "failed trades" is only intended for debugging and backtesting purposes.
    ///
    /// # Arguments
    /// * `trade` - The failed trade to add
    fn add_failed_trade(&mut self, trade: FailedTrade) {
        let row = trade.as_dataframe();
        self.failed_trades = self.failed_trades.vstack(&row).unwrap();
    }

    /// Add an executed trade to the portfolio
    ///
    /// Adding an executed trade will update the capital and assets of the portfolio.
    ///
    /// # Arguments
    /// * `trade` - The executed trade to add
    fn add_executed_trade(&mut self, trade: ExecutedTrade) {
        if trade.get_side() == Side::Buy {
            self.decrease_capital(trade.get_cost(), *trade.get_point());
            self.increase_assets(trade.get_quantity(), *trade.get_point());
            self.add_open_position(&trade);
        } else{
            self.increase_capital(trade.get_cost(), *trade.get_point());
            self.decrease_assets(trade.get_quantity(), *trade.get_point());
            self.clear_open_positions(&trade)
        }
        let row = trade.as_dataframe();
        self.executed_trades = self.executed_trades.vstack(&row).unwrap();
    }

    /// Check if a given rate is profitable to sell assets, then return a `FutureTrade` with the appropriate
    /// side and quantity.
    ///
    /// This method is used to determine if a given rate is profitable to sell assets at. If it is, then
    /// open positions are selected with the `Portfolio::select_open_positions()` method and the quantity
    /// of the trade is set to the sum of the quantities of the selected open positions.
    ///
    /// # Arguments
    /// * `rate` - The proposed rate to check
    ///
    /// # Returns
    /// * `Some` - A `FutureTrade` with the appropriate side, quantity, and rate
    /// * `None` - If the rate is not profitable
    fn is_rate_profitable(&self, rate: f64) -> Option<FutureTrade> {
        let viable_positions = self.select_open_positions(rate);

        if let Some(positions) = viable_positions {
            let quantity: f64 = positions
                .column("quantity").unwrap()
                .sum().unwrap();
            let cost: f64 = positions
                .column("cost").unwrap()
                .sum().unwrap();

            let sell_value = quantity * rate;
            let profit = sell_value - cost;
            if profit > self.threshold {
                let time = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap();
                return Some(FutureTrade::new(
                    Side::Sell,
                    rate,
                    quantity,
                    time
                ))
            }
        }
        None
    }
}


#[cfg(test)]
mod tests {
    use chrono::{
        NaiveDateTime, Utc
    };
    use crate::portfolio::Portfolio;
    use crate::portfolio::assets::AssetHandlers;
    use crate::portfolio::capital::CapitalHandlers;
    use crate::portfolio::trade::TradeHandlers;
    use crate::types::{
        ReasonCode, Side, ExecutedTrade, FailedTrade, Trade
    };

    /// Test that a failed trade is correctly added to the portfolio storage.
    /// Since the failed storage is only for debugging and backtestesting, no other checks
    /// are necessary.
    #[test]
    fn test_add_failed_trade() {
        let mut portfolio = Portfolio::new(100.0, 100.0, None);
        assert!(portfolio.failed_trades.is_empty());

        // add a failed buy
        let trade = FailedTrade::new(
            ReasonCode::Unknown,
            Side::Buy,
            100.0,
            1.0,
            NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap()
        );
        portfolio.add_failed_trade(trade);
        assert_eq!(portfolio.failed_trades.height(), 1);

        // add a failed sell
        let trade = FailedTrade::new(
            ReasonCode::Unknown,
            Side::Sell,
            100.0,
            1.0,
            NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap()
        );
        portfolio.add_failed_trade(trade);
        assert_eq!(portfolio.failed_trades.height(), 2);
    }

    /// Test that an executed trade is correctly added to the portfolio storage
    /// and that the capital and assets are updated appropriately
    #[test]
    fn test_add_executed_trade() {
        let mut portfolio = Portfolio::new(200.0, 200.0, None);

        // handle a buy
        let trade = ExecutedTrade::new(
            "id".to_string(),
            Side::Buy,
            100.0,
            1.0,
            NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap()
        );
        assert!(portfolio.executed_trades.is_empty());
        assert_eq!(portfolio.get_capital(), 200.0);
        assert_eq!(portfolio.get_assets(), 200.0);

        portfolio.add_executed_trade(trade);
        assert_eq!(portfolio.executed_trades.height(), 1);
        assert_eq!(portfolio.open_positions.len(), 1);

        // check that capital and assets are updated
        assert_eq!(portfolio.get_capital(), 100.0);
        assert_eq!(portfolio.get_assets(), 201.0);

        // handle a sell
        let trade = ExecutedTrade::new(
            "id".to_string(),
            Side::Sell,
            100.0,
            1.0,
            NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap()
        );

        portfolio.add_executed_trade(trade);
        assert_eq!(portfolio.executed_trades.height(), 2);

        // check that capital and assets are updated
        assert_eq!(portfolio.get_capital(), 200.0);
        assert_eq!(portfolio.get_assets(), 200.0);
        assert_eq!(portfolio.open_positions.len(), 0);
    }

    #[test]
    fn test_is_rate_profitable() {
        let mut portfolio = Portfolio::new(200.0, 200.0, None);
        let trade_price = 100.0;

        // check that a rate is not profitable when there are no open positions
        assert!(portfolio.is_rate_profitable(trade_price).is_none());

        // add an open position
        let price = 90.0;
        let quantity = 1.0;
        let trade = ExecutedTrade::new(
            "id".to_string(),
            Side::Buy,
            price,
            quantity,
            NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap()
        );
        portfolio.add_executed_trade(trade);
        assert_eq!(portfolio.open_positions.len(), 1);
        assert_eq!(portfolio.executed_trades.height(), 1);

        // check that a properly formatted trade is returned when rate is profitable
        let proposed_trade = portfolio.is_rate_profitable(trade_price).unwrap();
        assert_eq!(proposed_trade.get_quantity(), quantity);
        assert_eq!(proposed_trade.get_price(), trade_price);

        // assert that a non-profitable trade is not included in the proposed trade
        let trade2 = ExecutedTrade::new(
            "id".to_string(),
            Side::Buy,
            100.1,
            quantity,
            NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap()
        );
        portfolio.add_executed_trade(trade2);

        // check that the non-profitable trade is not included
        let proposed_trade = portfolio.is_rate_profitable(trade_price).unwrap();
        assert_eq!(proposed_trade.get_price(), trade_price);
        assert_eq!(proposed_trade.get_quantity(), quantity);

        let trade3 = ExecutedTrade::new(
            "id".to_string(),
            Side::Buy,
            90.0,
            quantity,
            NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap()
        );
        portfolio.add_executed_trade(trade3);

        let proposed_trade = portfolio.is_rate_profitable(trade_price).unwrap();
        assert_eq!(proposed_trade.get_price(), trade_price);
        assert_eq!(proposed_trade.get_quantity(), quantity * 2.0);
    }
}