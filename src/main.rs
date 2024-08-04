use std::time::Instant;
use chrono::{DateTime, NaiveDateTime};
use log::info;
use crate::traits::AsDataFrame;
use crate::backtesting::{BacktestingConfig, BacktestingRunner};
use crate::portfolio::PortfolioArgs;

use polars::prelude::*;
use rust_decimal_macros::dec;
use crate::markets::utils::save_candles;
use crate::types::MarketData;
use crate::utils::print_candle_statistics;

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
mod processor;

fn main() {
    colog::init();

    let strategy = strategies::Strategy::new(
        vec![Box::new(indicators::BBands::default())],
        strategies::Consensus::Unison,
    );

    let mut runner = BacktestingRunner::from_config(
        "data/backtesting_config.toml",
        strategy
    );

    info!("Starting to process");
    runner.run().unwrap();
}