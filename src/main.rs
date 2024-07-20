use crate::traits::AsDataFrame;
use polars::prelude::*;
use rust_decimal_macros::dec;
use crate::backtesting::BacktestingRunner;
use crate::portfolio::PortfolioArgs;

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
    let db_path = "data/candle_data.sqlite3";
    let table_name = "coinbase_SHIBUSD_1m_candles";

    let candles = utils::extract_candles_from_db(db_path, table_name).unwrap().as_dataframe();

    let strategy = strategies::Strategy::new(
        vec![Box::new(indicators::BBands::default())],
        strategies::Consensus::Unison,
    );

    let mut runner = BacktestingRunner::new(
        strategy,
        PortfolioArgs {
        assets: dec!(0.0),
        capital: dec!(100.0),
        threshold: dec!(0.0),
        ..Default::default()
    });

    let performance = runner.run(&candles).unwrap();

    println!("Performance: {:?}", performance);
}