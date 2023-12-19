use crate::portfolio::assets::AssetHandlers;
use crate::portfolio::capital::CapitalHandlers;
use crate::portfolio::Portfolio;
use crate::traits::AsDataFrame;
use crate::types::signals::Side;
use crate::types::trades::{executed::ExecutedTrade, failed::FailedTrade, Trade};

/// Interface methods for storing trades
pub trait TradeHandlers: AssetHandlers + CapitalHandlers {
    fn add_failed_trade(&mut self, trade: FailedTrade);
    fn add_executed_trade(&mut self, trade: ExecutedTrade);
}

impl TradeHandlers for Portfolio {
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
        } else {
            self.increase_capital(trade.get_cost(), *trade.get_point());
            self.decrease_assets(trade.get_quantity(), *trade.get_point());
        }
        let row = trade.as_dataframe();
        self.executed_trades = self.executed_trades.vstack(&row).unwrap();
    }
}


#[cfg(test)]
mod tests {
    use chrono::NaiveDateTime;
    use crate::portfolio::assets::AssetHandlers;
    use crate::portfolio::capital::CapitalHandlers;
    use crate::types::reason_code::ReasonCode;

    /// Test that a failed trade is correctly added to the portfolio storage.
    /// Since the failed storage is only for debugging and backtestesting, no other checks
    /// are necessary.
    #[test]
    fn test_add_failed_trade() {
        use crate::types::signals::Side;
        use crate::types::trades::failed::FailedTrade;
        use crate::portfolio::trade::TradeHandlers;
        use crate::portfolio::Portfolio;
        use chrono::Utc;

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
        use crate::types::signals::Side;
        use crate::types::trades::executed::ExecutedTrade;
        use crate::portfolio::trade::TradeHandlers;
        use crate::portfolio::Portfolio;
        use chrono::Utc;

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
    }
}