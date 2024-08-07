use crate::backtesting::BacktestingRuntime;
use log::info;

mod backtesting;
mod indicators;
mod manager;
mod markets;
mod portfolio;
mod processor;
mod risk;
mod serialization;
mod strategies;
mod traits;
mod types;
mod utils;

fn main() {
    colog::init();

    let strategy = strategies::Strategy::new(
        vec![
            Box::new(indicators::BBands::default()),
            Box::new(indicators::VWAP::new(5)),
        ],
        strategies::Consensus::Unison,
    );

    let mut runtime = BacktestingRuntime::from_config("data/backtesting_config.toml", strategy)
        .load_candles()
        .expect("Could not load candles");

    info!("******************************************\nStarting to process");
    runtime.run().unwrap();

    // Save runtime data
    info!("******************************************\nSaving backtesting runtime data");
    runtime.save_data("data/backtesting");
}
