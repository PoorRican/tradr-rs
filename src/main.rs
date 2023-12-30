use std::path::Path;
use tokio::time::sleep;

mod indicators;
mod markets;
mod portfolio;
mod strategies;
mod time;
mod traits;
mod types;
mod engine;
mod utils;
mod timing;


#[tokio::main]
async fn main() {
    let market = markets::CoinbaseClient::new()
        .disable_trades();
    let portfolio = portfolio::Portfolio::new(0.0, 100.0, None);
    let strategy = strategies::Strategy::new(vec![
        Box::new(indicators::BBands::new(20, 2.0)),
    ], strategies::Consensus::Unison);
    let mut engine = engine::Engine::new(
        "5m",
       portfolio,
        strategy,
        "DOGE-USD",
        &market);


    // setup path
    let path = Path::new("data");

    // bootstrap
    engine.bootstrap().await;
    println!("Completed bootstrapping...");

    // save current data
    // TODO: attempt to load data
    engine.save(path)
        .expect("Failed to save data");
    println!("Saved data...");


    // run the engine once every 5 minutes
    // TODO: sync with candles. ie: when time is a multiple of 5 minutes
    loop {
        engine.run().await;
        engine.save(path).unwrap();
        sleep(core::time::Duration::from_secs(60*5)).await;
        println!("Ran 1 iteration");
    }
}
