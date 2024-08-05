use log::info;
use crate::backtesting::BacktestingRuntime;

mod backtesting;
mod indicators;
mod markets;
mod portfolio;
mod serialization;
mod strategies;
mod traits;
mod types;
mod utils;
mod risk;
mod manager;
mod processor;

fn main() {
    colog::init();

    let strategy = strategies::Strategy::new(
        vec![
            Box::new(indicators::BBands::default()),
            Box::new(indicators::VWAP::new(5)),
        ],
        strategies::Consensus::Unison,
    );

    let mut runtime = BacktestingRuntime::from_config(
        "data/backtesting_config.toml",
        strategy
    ).load_candles().expect("Could not load candles");

    info!("Starting to process");
    runtime.run().unwrap();

    // Save runtime data
    runtime.save_data();
}