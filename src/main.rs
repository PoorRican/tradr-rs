use std::path::Path;
use std::time::Duration;
use tokio::time::sleep;
use crate::timing::wait_until;

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
        "BTC-USD",
        &market);


    // setup path
    let path = Path::new("data");

    // bootstrap
    engine.bootstrap().await;
    engine.save(path)
        .expect("Failed to save data");
    println!("Completed bootstrapping and saved data...");
    print_time();

    // print last candle time
    let last_candle_time = engine.last_candle_time();
    println!("The last candle retrieved is {last_candle_time}");

    // wait until the next candle is released
    println!("Waiting until next interval...");
    wait_until(INTERVAL).await;

    // run the engine once per interval
    println!("\nBeginning loop...");
    print_time();

    loop {
        // if there is no new data, wait 5 seconds and try again
        // certain intervals might not have data available and can be significantly delayed.
        // therefore, in order to catch as much data as possible, a wait time is necessary
        // in order to prevent the next update from returning more than one row
        while !engine.run().await {
            let wait = 20;
            eprintln!("No new data available. Retrying in {wait} seconds");
            sleep(Duration::from_secs(wait)).await;
        }
        print_time();
        println!("Ran 1 iteration");
        engine.save(path).unwrap();
        wait_until(INTERVAL).await;
    }
}

fn print_time() {
    let time = chrono::Utc::now();
    println!("The time is now {time}");
}