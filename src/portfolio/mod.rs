mod assets;
mod capital;
mod persistence;
mod position;
mod tracked;
mod trade;

pub use assets::AssetHandlers;
pub use capital::CapitalHandlers;
pub use persistence::Persistence;
pub use position::PositionHandlers;
pub use trade::TradeHandlers;

use crate::markets::FeeCalculator;
use crate::portfolio::tracked::TrackedValue;
use chrono::{Duration, NaiveDateTime, Utc};
use polars::prelude::DataFrame;

pub const DEFAULT_LIMIT: usize = 4;
pub const DEFAULT_TIMEOUT_MINUTES: i64 = 60 * 2;
pub const DEFAULT_THRESHOLD: f64 = 0.50;

/// This struct is used to manage an entire portfolio for a given asset.
///
/// It is responsible for managing the assets and capital available to the portfolio,
/// as well as the open positions and executed trades.
pub struct Portfolio {
    failed_trades: DataFrame,
    executed_trades: DataFrame,
    open_positions: Vec<NaiveDateTime>,

    threshold: f64,
    assets_ts: TrackedValue,
    capital_ts: TrackedValue,
    open_positions_limit: usize,
    timeout: Duration,

    fee_calculator: Option<Box<dyn FeeCalculator>>,
}

impl Portfolio {
    pub fn new<T>(assets: f64, capital: f64, point: T) -> Portfolio
    where
        T: Into<Option<NaiveDateTime>>,
    {
        let point = point.into().unwrap_or_else(|| {
            NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap()
        });

        Portfolio {
            failed_trades: DataFrame::empty(),
            executed_trades: DataFrame::empty(),
            open_positions: vec![],

            threshold: DEFAULT_THRESHOLD,
            assets_ts: TrackedValue::with_initial(assets, point),
            capital_ts: TrackedValue::with_initial(capital, point),
            open_positions_limit: DEFAULT_LIMIT,
            timeout: Duration::minutes(DEFAULT_TIMEOUT_MINUTES),
            fee_calculator: None,
        }
    }

    /// Constructor with loaded data
    pub fn with_data(
        failed_trades: DataFrame,
        executed_trades: DataFrame,
        open_positions: Vec<NaiveDateTime>,
        assets_ts: TrackedValue,
        capital_ts: TrackedValue,
    ) -> Portfolio {
        Portfolio {
            failed_trades,
            executed_trades,
            open_positions,
            threshold: DEFAULT_THRESHOLD,
            assets_ts,
            capital_ts,
            open_positions_limit: DEFAULT_LIMIT,
            timeout: Duration::minutes(DEFAULT_TIMEOUT_MINUTES),
            fee_calculator: None,
        }
    }

    /// Builder method for the `fee_calculator` field
    pub fn add_fee_calculator<T>(mut self, fee_calculator: T) -> Self
    where
        T: FeeCalculator + 'static,
    {
        self.fee_calculator = Some(Box::new(fee_calculator));
        self
    }

    /// Setter for the profitability threshold parameter
    ///
    /// # Arguments
    /// * `threshold` - The new profitability threshold in unit currency
    pub fn set_threshold(&mut self, threshold: f64) {
        self.threshold = threshold;
    }

    /// Setter for the open positions limit parameter
    ///
    /// This is used by `Portfolio::available_open_positions()` to determine the number of
    /// available open positions at any given time.
    ///
    /// # Arguments
    /// * `limit` - The number of open positions allowed at any given time
    pub fn set_open_positions_limit(&mut self, limit: usize) {
        self.open_positions_limit = limit;
    }

    /// Setter for the open positions timeout parameter
    ///
    /// # Arguments
    /// * `minute` - The number of minutes after which an open position is closed
    pub fn set_timeout(&mut self, minute: usize) {
        self.timeout = Duration::minutes(minute as i64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::portfolio::{assets::AssetHandlers, capital::CapitalHandlers};
    use crate::types::{ExecutedTrade, FailedTrade, FutureTrade, ReasonCode, Side, Trade};
    use std::collections::VecDeque;

    #[test]
    fn test_with_data() {
        use crate::types::Side;
        use chrono::NaiveDateTime;

        let assets = 100.0;
        let capital = 100.0;
        let point = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap();

        let mut portfolio = Portfolio::new(assets, capital, point);
        let trade = FutureTrade::new(Side::Buy, 100.0, 1.0, point + Duration::seconds(1));
        let executed_trade = ExecutedTrade::with_future_trade("id".to_string(), trade.clone());
        let failed_trade =
            FailedTrade::with_future_trade(ReasonCode::MarketRejection, trade.clone());

        portfolio.add_executed_trade(executed_trade);
        portfolio.add_failed_trade(failed_trade);

        let portfolio = Portfolio::with_data(
            portfolio.failed_trades,
            portfolio.executed_trades,
            portfolio.open_positions,
            portfolio.assets_ts,
            portfolio.capital_ts,
        );

        // assert that assets and capital `TrackedValues` were initialized correctly
        assert_eq!(portfolio.get_assets(), assets + 1.0);
        assert_eq!(portfolio.get_capital(), capital - 100.0);

        // assert that the default parameters are set correctly
        assert_eq!(portfolio.threshold, DEFAULT_THRESHOLD);
        assert_eq!(portfolio.open_positions_limit, DEFAULT_LIMIT);
        assert_eq!(
            portfolio.timeout,
            Duration::minutes(DEFAULT_TIMEOUT_MINUTES)
        );

        // assert that the trade storage is empty
        assert_eq!(portfolio.executed_trades.height(), 1);
        assert_eq!(portfolio.failed_trades.height(), 1);
        assert_eq!(portfolio.open_positions.len(), 1);
    }

    #[test]
    fn test_new() {
        use chrono::NaiveDateTime;

        let assets = 100.0;
        let capital = 100.0;
        let point = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap();

        let portfolio = Portfolio::new(assets, capital, point);

        // assert that assets and capital `TrackedValues` are initialized correctly
        assert_eq!(portfolio.get_assets(), assets);
        assert_eq!(portfolio.get_capital(), capital);

        // assert that the default parameters are set correctly
        assert_eq!(portfolio.threshold, DEFAULT_THRESHOLD);
        assert_eq!(portfolio.open_positions_limit, DEFAULT_LIMIT);
        assert_eq!(
            portfolio.timeout,
            Duration::minutes(DEFAULT_TIMEOUT_MINUTES)
        );

        // assert that the trade storage is empty
        assert!(portfolio.failed_trades.is_empty());
        assert!(portfolio.executed_trades.is_empty());
        assert!(portfolio.open_positions.is_empty());
    }

    #[test]
    fn test_add_fee_calculator() {
        use crate::markets::SimplePercentageFee;
        let portfolio = Portfolio::new(100.0, 100.0, None);
        assert!(portfolio.fee_calculator.is_none());

        let portfolio = portfolio.add_fee_calculator(SimplePercentageFee::new(0.8));
        assert!(portfolio.fee_calculator.is_some());
    }

    #[test]
    fn test_set_threshold() {
        let mut portfolio = Portfolio::new(100.0, 100.0, None);
        assert_eq!(portfolio.threshold, DEFAULT_THRESHOLD);

        portfolio.set_threshold(0.25);
        assert_eq!(portfolio.threshold, 0.25);
    }

    #[test]
    fn test_set_open_positions_limit() {
        let mut portfolio = Portfolio::new(100.0, 100.0, None);
        assert_eq!(portfolio.open_positions_limit, DEFAULT_LIMIT);

        portfolio.set_open_positions_limit(2);
        assert_eq!(portfolio.open_positions_limit, 2);
    }

    #[test]
    fn test_set_timeout() {
        let mut portfolio = Portfolio::new(100.0, 100.0, None);
        assert_eq!(
            portfolio.timeout,
            Duration::minutes(DEFAULT_TIMEOUT_MINUTES)
        );

        portfolio.set_timeout(10);
        assert_eq!(portfolio.timeout, Duration::minutes(10));
    }

    /// Simulates a typical scenario in which a portfolio is created, and then
    /// a series of trades are executed, some of which are profitable and some of which are not.
    /// Check to make sure that the portfolio is updated appropriately.
    #[test]
    fn market_simulation() {
        let time = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap()
            - Duration::seconds(1);

        let mut portfolio = Portfolio::new(0.0, 300.0, time - Duration::seconds(1));
        assert_eq!(portfolio.get_assets(), 0.0);
        assert_eq!(portfolio.get_capital(), 300.0);

        // this will be the sequences of prices used to simulate the market
        let mut prices = VecDeque::from_iter(&[
            100.0, // buy
            99.0,  // buy
            98.0,  // attempt sell
            97.0,  // buy
            98.0,  // sell
            101.0, // sell
        ]);

        /*********************
        handle the first buy
        *********************/

        // simulate a buy order
        let price = prices.pop_front().unwrap();
        let trade = ExecutedTrade::new_without_cost(
            "id".to_string(),
            Side::Buy,
            *price,
            1.0,
            time + Duration::milliseconds(1),
        );
        portfolio.add_executed_trade(trade);

        // assert that capital and assets have changed accordingly
        assert_eq!(portfolio.get_assets(), 1.0);
        assert_eq!(portfolio.get_capital(), 200.0);

        // assert that trade storage, open positions, and available open positions have been updated
        assert_eq!(portfolio.get_executed_trades().height(), 1);
        assert_eq!(portfolio.get_open_positions().unwrap().height(), 1);
        assert_eq!(portfolio.available_open_positions(), DEFAULT_LIMIT - 1);

        /**********************
        handle the second buy
        **********************/

        // simulate another buy order at a lower price than the first
        let price = prices.pop_front().unwrap();
        let trade = ExecutedTrade::new_without_cost(
            "id".to_string(),
            Side::Buy,
            *price,
            1.0,
            time + Duration::milliseconds(2),
        );
        portfolio.add_executed_trade(trade);

        // assert that capital and assets have changed accordingly
        assert_eq!(portfolio.get_assets(), 2.0);
        assert_eq!(portfolio.get_capital(), 101.0);

        // assert that trade storage, open positions, and available open positions have been updated
        assert_eq!(portfolio.get_executed_trades().height(), 2);
        assert_eq!(portfolio.get_open_positions().unwrap().height(), 2);
        assert_eq!(portfolio.available_open_positions(), DEFAULT_LIMIT - 2);

        /*****************************
        attempt an unprofitable sell
        *****************************/

        // attempt to generate a sell order using `is_rate_profitable` at a rate which is not profitable
        let price = prices.pop_front().unwrap();
        let potential_trade =
            FutureTrade::new(Side::Sell, *price, 1.0, time + Duration::milliseconds(3));
        let result = portfolio.is_rate_profitable(potential_trade.get_price());

        // assert that there is no proposed trade
        assert!(result.is_none());

        // assert that capital and assets have not changed
        assert_eq!(portfolio.get_assets(), 2.0);
        assert_eq!(portfolio.get_capital(), 101.0);

        // assert that trade storage, open positions, and available open positions have not been updated
        assert_eq!(portfolio.get_executed_trades().height(), 2);
        assert_eq!(portfolio.get_open_positions().unwrap().height(), 2);
        assert_eq!(portfolio.available_open_positions(), DEFAULT_LIMIT - 2);

        /*********************
        handle the third buy
        *********************/

        // simulate another buy order at a lower price than the second
        let price = prices.pop_front().unwrap();
        let trade = ExecutedTrade::new_without_cost(
            "id".to_string(),
            Side::Buy,
            *price,
            1.0,
            time + Duration::milliseconds(4),
        );
        portfolio.add_executed_trade(trade);

        // assert that capital and assets have changed accordingly
        assert_eq!(portfolio.get_assets(), 3.0);
        assert_eq!(portfolio.get_capital(), 4.0);

        // assert that trade storage, open positions, and available open positions have been updated
        assert_eq!(portfolio.get_executed_trades().height(), 3);
        assert_eq!(portfolio.get_open_positions().unwrap().height(), 3);
        assert_eq!(portfolio.available_open_positions(), DEFAULT_LIMIT - 3);

        /**************************
        attempt a profitable sell
        **************************/

        // generate a sell order using `is_rate_profitable` at a rate which would sell the third buy order
        let price = prices.pop_front().unwrap();
        let potential_trade = portfolio.is_rate_profitable(*price).unwrap();

        assert_eq!(potential_trade.get_side(), Side::Sell);
        assert_eq!(potential_trade.get_price(), *price);
        assert_eq!(potential_trade.get_quantity(), 1.0);

        let trade = ExecutedTrade::with_future_trade("id".to_string(), potential_trade);
        portfolio.add_executed_trade(trade);

        // assert that capital and assets have changed accordingly
        assert_eq!(portfolio.get_assets(), 2.0);
        assert_eq!(portfolio.get_capital(), 102.0);

        // assert that trade storage, open positions, and available open positions have been updated
        assert_eq!(portfolio.get_executed_trades().height(), 4);
        assert_eq!(portfolio.get_open_positions().unwrap().height(), 2);
        assert_eq!(portfolio.available_open_positions(), DEFAULT_LIMIT - 2);

        /****************************
        sell the rest of the assets
        ****************************/

        // simulate a sell order that will sell the first and second buy
        let price = prices.pop_front().unwrap();
        let potential_trade = portfolio.is_rate_profitable(*price).unwrap();

        assert_eq!(potential_trade.get_side(), Side::Sell);
        assert_eq!(potential_trade.get_price(), *price);
        assert_eq!(potential_trade.get_quantity(), 2.0);

        let trade = ExecutedTrade::with_future_trade("id".to_string(), potential_trade);
        portfolio.add_executed_trade(trade);

        // assert that capital and assets have changed accordingly
        assert_eq!(portfolio.get_assets(), 0.0);
        assert_eq!(portfolio.get_capital(), 304.0);

        // assert that trade storage, open positions, and available open positions have been updated
        assert_eq!(portfolio.get_executed_trades().height(), 5);
        assert!(portfolio.get_open_positions().is_none());
        assert_eq!(portfolio.available_open_positions(), DEFAULT_LIMIT);
    }
}
