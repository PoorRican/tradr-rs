extern crate tradr;

use std::collections::VecDeque;
use chrono::{Duration, NaiveDateTime, Utc};
use tradr::portfolio::{
    DEFAULT_LIMIT, Portfolio,
    CapitalHandlers, AssetHandlers, TradeHandlers, PositionHandlers
};
use tradr::types::{
    Side, ExecutedTrade, FutureTrade, Trade
};

/// Simulates a typical scenario in which a portfolio is created, and then
/// a series of trades are executed, some of which are profitable and some of which are not.
/// Check to make sure that the portfolio is updated appropriately.
#[test]
fn market_simulation() {
    let time = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap() - Duration::seconds(1);

    let mut portfolio = Portfolio::new(0.0, 300.0, time - Duration::seconds(1));
    assert_eq!(portfolio.get_assets(), 0.0);
    assert_eq!(portfolio.get_capital(), 300.0);

    // this will be the sequences of prices used to simulate the market
    let mut prices = VecDeque::from_iter(&[
        100.0,  // buy
        99.0,   // buy
        98.0,   // attempt sell
        97.0,   // buy
        98.0,   // sell
        101.0,  // sell
    ]);

    /*********************
      handle the first buy
      *********************/

    // simulate a buy order
    let price = prices.pop_front().unwrap();
    let trade = ExecutedTrade::new(
        "id".to_string(),
        Side::Buy,
        *price,
        1.0,
        time + Duration::milliseconds(1)
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
    let trade = ExecutedTrade::new(
        "id".to_string(),
        Side::Buy,
        *price,
        1.0,
        time + Duration::milliseconds(2)
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
    let potential_trade = FutureTrade::new(
        Side::Sell,
        *price,
        1.0,
        time + Duration::milliseconds(3)
    );
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
    let trade = ExecutedTrade::new(
        "id".to_string(),
        Side::Buy,
        *price,
        1.0,
        time + Duration::milliseconds(4)
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

    let trade = ExecutedTrade::with_future_trade(
        "id".to_string(),
        potential_trade,
    );
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

    let trade = ExecutedTrade::with_future_trade(
        "id".to_string(),
        potential_trade,
    );
    portfolio.add_executed_trade(trade);

    // assert that capital and assets have changed accordingly
    assert_eq!(portfolio.get_assets(), 0.0);
    assert_eq!(portfolio.get_capital(), 304.0);

    // assert that trade storage, open positions, and available open positions have been updated
    assert_eq!(portfolio.get_executed_trades().height(), 5);
    assert!(portfolio.get_open_positions().is_none());
    assert_eq!(portfolio.available_open_positions(), DEFAULT_LIMIT);
}