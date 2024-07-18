use crate::portfolio::assets::AssetHandlers;
use crate::portfolio::capital::CapitalHandlers;
use crate::portfolio::position::PositionHandlers;
use crate::portfolio::Portfolio;
use crate::traits::AsDataFrame;
use crate::types::{Candle, ExecutedTrade, FailedTrade, FutureTrade, Side, Trade};
use chrono::{NaiveDateTime, Utc};
use polars::prelude::DataFrame;

/// Interface methods for storing and retrieving trades, and determining when to trade
pub trait TradeHandlers: PositionHandlers + AssetHandlers + CapitalHandlers {
    fn get_executed_trades(&self) -> &DataFrame;
    fn add_failed_trade(&mut self, trade: FailedTrade);
    fn add_executed_trade(&mut self, trade: ExecutedTrade);
    #[deprecated(note="Use generate_sell_opt() instead")]
    fn is_rate_profitable(&self, rate: f64) -> Option<FutureTrade>;
    fn generate_sell_opt(&self, candle: &Candle) -> Option<FutureTrade>;
    fn generate_buy_opt(&self, candle: &Candle) -> Option<FutureTrade>;
    fn get_buy_cost(&self) -> f64;
    fn get_last_trade(&self) -> Option<ExecutedTrade>;
    fn able_to_buy(&self) -> bool;
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
        } else {
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
            // the total quantity of assets to be sold
            let quantity: f64 = positions.column("quantity").unwrap().sum().unwrap();

            // the total cost at which the assets were purchased
            let cost: f64 = positions.column("cost").unwrap().sum().unwrap();

            // calculate the value of the assets at the proposed rate
            let sell_value = match self.fee_calculator {
                Some(ref fee_calculator) => {
                    fee_calculator.cost_including_fee(quantity * rate, Side::Sell)
                }
                None => quantity * rate,
            };
            let profit = sell_value - cost;
            if profit > self.threshold {
                let time = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap();
                return Some(FutureTrade::new(Side::Sell, rate, quantity, time));
            }
        }
        None
    }

    fn generate_sell_opt(&self, candle: &Candle) -> Option<FutureTrade> {
        let rate = calculate_sell_rate(candle);
        let viable_positions = self.select_open_positions(rate);

        if let Some(positions) = viable_positions {
            // the total quantity of assets to be sold
            let quantity: f64 = positions.column("quantity").unwrap().sum().unwrap();

            // the total cost at which the assets were purchased
            let cost: f64 = positions.column("cost").unwrap().sum().unwrap();

            // calculate the value of the assets at the proposed rate
            let sell_value = match self.fee_calculator {
                Some(ref fee_calculator) => {
                    fee_calculator.cost_including_fee(quantity * rate, Side::Sell)
                }
                None => quantity * rate,
            };
            let profit = sell_value - cost;
            if profit > self.threshold {
                return Some(FutureTrade::new(Side::Sell, rate, quantity, candle.time));
            }
        }
        None
    }

    fn generate_buy_opt(&self, candle: &Candle) -> Option<FutureTrade> {
        if !self.able_to_buy() {
            return None
        }
        let rate = calculate_buy_rate(candle);
        let cost = self.get_buy_cost();
        Some(FutureTrade::new(Side::Buy, rate, cost, candle.time))
    }


    /// The amount of capital to use for a single buy trade
    ///
    /// This number is determined by the amount of capital available and the number of open positions.
    fn get_buy_cost(&self) -> f64 {
        self.get_capital() / self.available_open_positions() as f64
    }

    /// Get the most recent trade
    ///
    /// # Returns
    /// * `Some` - The most recent trade
    /// * `None` - If there are no trades
    fn get_last_trade(&self) -> Option<ExecutedTrade> {
        if self.executed_trades.height() > 0 {
            let last_row = self.executed_trades.tail(Some(1));
            let trade = ExecutedTrade::from_row(&last_row);
            Some(trade)
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
        if self.available_open_positions() == 0 {
            return false;
        } else {
            // check the last trade
            let last_trade = self.get_last_trade();
            if let Some(trade) = last_trade {
                if trade.get_side() == Side::Buy {
                    // check timeout
                    let now = Utc::now().naive_utc();
                    let diff = now - *trade.get_point();
                    return if diff >= self.timeout { true } else { false };
                }
            }
            // if there was no last trade, or if the last trade was a sell, then we are able to buy
            true
        }
    }
}

fn calculate_buy_rate(candle: &Candle) -> f64 {
    ((candle.close * 2.0) + candle.high + candle.open) / 4.0
}

fn calculate_sell_rate(candle: &Candle) -> f64 {
    ((candle.close * 2.0) + candle.low + candle.open) / 4.0
}

#[cfg(test)]
mod tests {
    use crate::portfolio::{AssetHandlers, CapitalHandlers, Portfolio, TradeHandlers};
    use crate::types::{ExecutedTrade, FailedTrade, ReasonCode, Side, Trade};
    use chrono::{Duration, NaiveDateTime, Utc};

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
            NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap(),
        );
        portfolio.add_failed_trade(trade);
        assert_eq!(portfolio.failed_trades.height(), 1);

        // add a failed sell
        let trade = FailedTrade::new(
            ReasonCode::Unknown,
            Side::Sell,
            100.0,
            1.0,
            NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap(),
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
        let trade = ExecutedTrade::new_without_cost(
            "id".to_string(),
            Side::Buy,
            100.0,
            1.0,
            NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap(),
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
        let trade = ExecutedTrade::new_without_cost(
            "id".to_string(),
            Side::Sell,
            100.0,
            1.0,
            NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap(),
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
        let trade = ExecutedTrade::new_without_cost(
            "id".to_string(),
            Side::Buy,
            price,
            quantity,
            NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap(),
        );
        portfolio.add_executed_trade(trade);
        assert_eq!(portfolio.open_positions.len(), 1);
        assert_eq!(portfolio.executed_trades.height(), 1);

        // check that a properly formatted trade is returned when rate is profitable
        let proposed_trade = portfolio.is_rate_profitable(trade_price).unwrap();
        assert_eq!(proposed_trade.get_quantity(), quantity);
        assert_eq!(proposed_trade.get_price(), trade_price);

        // assert that a non-profitable trade is not included in the proposed trade
        let trade2 = ExecutedTrade::new_without_cost(
            "id".to_string(),
            Side::Buy,
            100.1,
            quantity,
            NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap(),
        );
        portfolio.add_executed_trade(trade2);

        // check that the non-profitable trade is not included
        let proposed_trade = portfolio.is_rate_profitable(trade_price).unwrap();
        assert_eq!(proposed_trade.get_price(), trade_price);
        assert_eq!(proposed_trade.get_quantity(), quantity);

        let trade3 = ExecutedTrade::new_without_cost(
            "id".to_string(),
            Side::Buy,
            90.0,
            quantity,
            NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap(),
        );
        portfolio.add_executed_trade(trade3);

        let proposed_trade = portfolio.is_rate_profitable(trade_price).unwrap();
        assert_eq!(proposed_trade.get_price(), trade_price);
        assert_eq!(proposed_trade.get_quantity(), quantity * 2.0);
    }

    /// This test is identical to the one above, except that it uses a fee calculator
    #[test]
    fn test_is_rate_profitable_denies_trade() {
        let time = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap();
        let mut portfolio = Portfolio::new(200.0, 200.0, time - Duration::seconds(1))
            .add_fee_calculator(crate::markets::SimplePercentageFee::new(1.0));
        portfolio.set_threshold(1.0);

        // build a buy trade
        let trade = ExecutedTrade::new_without_cost("id".to_string(), Side::Buy, 100.0, 1.0, time);

        assert!(portfolio.is_rate_profitable(100.1).is_none());
    }

    #[test]
    fn test_last_trade() {
        let mut portfolio = Portfolio::new(200.0, 200.0, None);
        assert!(portfolio.get_last_trade().is_none());

        let trade = ExecutedTrade::new_without_cost(
            "id".to_string(),
            Side::Buy,
            100.0,
            1.0,
            Utc::now().naive_utc(),
        );
        portfolio.add_executed_trade(trade);

        let last_trade = portfolio.get_last_trade();
        assert!(portfolio.get_last_trade().is_some());

        // append another trade and assert that the last trade is this new trade
        let id = "id".to_string();
        let side = Side::Sell;
        let price = 121.0;
        let quantity = 1.0;
        let time = Utc::now().naive_utc();

        let trade = ExecutedTrade::new_without_cost("id".to_string(), side, price, quantity, time);
        portfolio.add_executed_trade(trade);

        let last_trade = portfolio.get_last_trade().unwrap();
        assert_eq!(last_trade.get_id(), &id);
        assert_eq!(last_trade.get_side(), side);
        assert_eq!(last_trade.get_price(), price);
        assert_eq!(last_trade.get_quantity(), quantity);
        assert_eq!(
            last_trade.get_point().timestamp_millis(),
            time.timestamp_millis()
        );
    }

    #[test]
    fn test_able_to_buy() {
        // test that we are able to buy when there are no open positions
        let portfolio = Portfolio::new(100.0, 100.0, None);
        assert!(portfolio.able_to_buy());

        // test that we are able to buy if the last trade is a sell
        let mut portfolio = Portfolio::new(100.0, 100.0, None);
        let trade = ExecutedTrade::new_without_cost(
            "id".to_string(),
            Side::Sell,
            100.0,
            1.0,
            Utc::now().naive_utc(),
        );
        portfolio.add_executed_trade(trade);

        assert!(portfolio.able_to_buy());

        // test that we are not able to buy if the last trade is a buy and the timeout has not expired
        let mut portfolio = Portfolio::new(100.0, 100.0, None);
        let trade = ExecutedTrade::new_without_cost(
            "id".to_string(),
            Side::Buy,
            100.0,
            1.0,
            Utc::now().naive_utc(),
        );
        portfolio.add_executed_trade(trade);

        assert!(!portfolio.able_to_buy());

        // test hat we are able to buy if the last trade is a buy and the timeout has expired
        let mut portfolio = Portfolio::new(100.0, 100.0, None);
        let trade = ExecutedTrade::new_without_cost(
            "id".to_string(),
            Side::Buy,
            100.0,
            1.0,
            Utc::now().naive_utc() - portfolio.timeout,
        );
        portfolio.add_executed_trade(trade);

        assert!(portfolio.able_to_buy());
    }
}
