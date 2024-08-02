use std::time::Instant;
use chrono::{DateTime, NaiveDateTime};
use log::info;
use crate::traits::AsDataFrame;
use crate::backtesting::BacktestingRunner;
use crate::portfolio::PortfolioArgs;

use polars::prelude::*;
use rust_decimal_macros::dec;
use crate::markets::utils::save_candles;

mod backtesting;
mod indicators;
mod markets;
mod portfolio;
mod serialization;
mod strategies;
mod timing;
mod traits;
mod types;
mod utils;
mod risk;
mod manager;


fn main() {
    colog::init();

    let db_path = "data/candle_data.sqlite3";
    let candle_table = "coinbase_SHIBUSD_5m_candles";
    let market_data_table = "coinbase_BTCUSD_5m_candles";

    let candles = utils::extract_candles_from_db(db_path, candle_table).unwrap().as_dataframe();
    let market_data = utils::extract_candles_from_db(db_path, market_data_table).unwrap().as_dataframe();

    let strategy = strategies::Strategy::new(
        vec![Box::new(indicators::BBands::default())],
        strategies::Consensus::Unison,
    );

    let portfolio_args = PortfolioArgs {
        assets: dec!(0.0),
        capital: dec!(100.0),
        threshold: dec!(0.0),
        ..Default::default()
    };
    let manager_config = manager::PositionManagerConfig {
        max_position_size: dec!(100.0),
        stop_loss_percentage: dec!(0.05),
        take_profit_percentage: dec!(0.1),
        max_beta: dec!(1.4),
        var_limit: dec!(10.0),
        min_sharpe_ratio: dec!(0.4),
        ..Default::default()
    };
    let mut runner = BacktestingRunner::new(
        strategy,
        portfolio_args,
        manager_config,
    );

    let start_time = Instant::now();
    info!("Starting to process");
    let performance = runner.run(&candles, &market_data).unwrap();
    let elapsed = start_time.elapsed();

    // print candle statistics
    let candle_start = candles.column("time")
        .unwrap()
        .datetime()
        .unwrap()
        .head(Some(1))
        .get(0)
        .unwrap();
    let candle_start = DateTime::from_timestamp_millis(candle_start).unwrap().naive_utc();
    let candle_end = candles.column("time")
        .unwrap()
        .datetime()
        .unwrap()
        .tail(Some(1))
        .get(0)
        .unwrap();
    let candle_end = DateTime::from_timestamp_millis(candle_end).unwrap().naive_utc();

    info!("Candles range: {:?} - {:?}", candle_start, candle_end);

    let candle_len = candles.height();
    println!("Finished processing {:?} rows in {:?}", candle_len, elapsed);
    println!("Avg. processing time per row: {:?}", elapsed / candle_len as u32);

    println!("Performance: {:?}", performance);


    // save candles and indicator graphs as CSV

    info!("Saving data to CSV");

    let candle_path = format!("data/{}.csv", candle_table);
    let market_data_path = format!("data/{}.csv", market_data_table);
    let bbands_path = "data/bbands_graph.csv";

    // create mutable versions of the DataFrames
    let mut candles = candles.clone();
    let mut market_data = market_data.clone();

    // save candles to CSV
    save_candles(&mut candles, &candle_path).unwrap();
    save_candles(&mut market_data, &market_data_path).unwrap();

    // save indicator graph as CSV
    let mut bbands = runner.get_strategy_as_mut().indicators[0].as_mut();
    let bbands_graph = bbands.save_graph_as_csv(bbands_path).unwrap();
}