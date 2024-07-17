use std::path::Path;
use std::time::Duration;
use tokio::time::sleep;
use crate::timing::wait_until;

mod indicators;
mod markets;
mod portfolio;
mod strategies;
mod serialization;
mod traits;
mod types;
mod engine;
mod utils;
mod timing;


#[tokio::main]
async fn main() {
    println!("Starting...\n");

    const INTERVAL: &str = "5m";

    let market = markets::CoinbaseClient::new()
        .disable_trades();
    let portfolio = portfolio::Portfolio::new(0.0, 100.0, None);
    let strategy = strategies::Strategy::new(vec![
        Box::new(indicators::BBands::new(20, 2.0)),
    ], strategies::Consensus::Unison);
    let mut engine = engine::Engine::new(
        INTERVAL,
        portfolio,
        strategy,
        "MATIC-USD",
        &market);


    // setup path
    let path = Path::new("data");

    // bootstrap
    engine.initialize().await;
    engine.save(path)
        .expect("Failed to save data");

    // wait until the next candle is released
    println!("Waiting until next interval...");
    wait_until(INTERVAL).await;

    // run the engine once per interval
    println!("\nBeginning loop...");
    print_time();

    loop {
        print!("\n");
        // if there is no new data, wait then try again
        // certain intervals might not have data available and can be significantly delayed.
        // therefore, in order to catch as much data as possible, a wait time is necessary
        // in order to prevent the next update from returning more than one row
        while !engine.run().await {
            let wait = 20;
            sleep(Duration::from_secs(wait)).await;
        }
        println!("Ran 1 iteration");
        print_time();
        engine.save(path).unwrap();
        wait_until(INTERVAL).await;
    }
}

fn print_time() {
    let time = chrono::Utc::now();
    println!("The time is now {time}");
}