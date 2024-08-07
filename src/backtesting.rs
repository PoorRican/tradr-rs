use std::path::{Path, PathBuf};
use std::time::{Instant, Duration};
use chrono::{DateTime};
use log::info;
use polars::prelude::*;
use rust_decimal::Decimal;
use serde::Deserialize;
use crate::manager::{PositionManager, PositionManagerConfig, PositionManagerError, TradeDecision};
use crate::markets::utils::save_candles;
use crate::portfolio::{CapitalHandlers, Portfolio, PortfolioArgs, PositionHandlers, TradeHandlers};
use crate::processor::CandleProcessor;
use crate::risk::{calculate_risk, RiskCalculationErrors};
use crate::strategies::Strategy;
use crate::types::{Candle, ExecutedTrade, FutureTrade, MarketData, MarketDataError, Side, Signal};
use crate::utils;
use crate::utils::{AlignmentError, check_candle_alignment, extract_candles_from_df, print_candle_statistics, trim_candles};

const CANDLE_TRIM_SIZE: IdxSize = 100;

/// Total configuration for backtesting
///
/// Meant to be read from a TOML config file
#[derive(Deserialize, Debug)]
pub struct BacktestingConfig {
    portfolio: PortfolioArgs,
    risk: PositionManagerConfig,
    trading: TradingConfig,
}

/// Contains trading config data for backtesting
///
/// Meant to be read from a TOML config file
#[derive(Deserialize, Debug)]
pub struct TradingConfig {
    frequency: String,
    trading_asset: String,
    market_asset: String,
}

#[derive(Debug)]
pub enum BacktestingErrors {
    APIError(String),
    CandleError(MarketDataError),
    /// Raised when unable to extract signals from trading asset data
    SignalExtractionError,
    AlignmentError(AlignmentError),

    RiskCalculationError(RiskCalculationErrors),
    DecisionError(PositionManagerError),
}

pub struct BacktestingRuntime {
    strategy: Strategy,
    portfolio_args: PortfolioArgs,
    manager_config: PositionManagerConfig,
    trading_config: TradingConfig,

    /// Global candle references
    market_candle_data: Option<MarketData>,
    trading_candle_data: Option<MarketData>,

    /// Usable candles for market data
    market_candles: Option<DataFrame>,

    /// Usable candles for trading data
    trading_candles: Option<DataFrame>,
}

impl BacktestingRuntime {
    pub fn new<S: Into<String>>(
        strategy: Strategy, portfolio_args: PortfolioArgs, manager_config: PositionManagerConfig,
        frequency: S, trading_asset: S, market_asset: S
    ) -> Self {
        let frequency = frequency.into();
        let trading_asset = trading_asset.into();
        let market_asset = market_asset.into();

        BacktestingRuntime {
            portfolio_args,
            strategy,
            manager_config,
            trading_config: TradingConfig {
                frequency,
                trading_asset,
                market_asset,
            },
            market_candle_data: None,
            trading_candle_data: None,
            market_candles: None,
            trading_candles: None,
        }
    }

    /// Read the backtesting configuration from the given TOML file
    ///
    /// # Arguments
    /// * `config_path` - The path to the TOML config file
    /// * `strategy` - The strategy to use for backtesting
    pub fn from_config(config_path: &str, strategy: Strategy) -> Self {
        let config_str = std::fs::read_to_string(config_path).unwrap();
        let config: BacktestingConfig = toml::from_str(&config_str).unwrap();

        BacktestingRuntime {
            portfolio_args: config.portfolio,
            strategy,
            manager_config: config.risk,
            trading_config: config.trading,
            market_candle_data: None,
            trading_candle_data: None,
            market_candles: None,
            trading_candles: None,
        }
    }

    pub fn load_candles(mut self) -> Result<Self, BacktestingErrors> {
        info!("******************************************\nLoading Candles");
        // load candle data
        self.market_candle_data = MarketData::from_db(&self.trading_config.market_asset).into();
        self.trading_candle_data = MarketData::from_db(&self.trading_config.trading_asset).into();

        // compute indicator graph
        let trading_candles = self.get_trading_asset()?.to_owned();

        self.strategy.process_candle(&trading_candles).unwrap();

        // populate market and trading candles
        self.trading_candles = trading_candles.into();
        self.market_candles = self.get_market_asset()?.to_owned().into();

        info!("Finished loading candles");

        Ok(self)
    }

    fn get_trading_asset(&self) -> Result<&DataFrame, BacktestingErrors> {
        if let Some(data) = self.trading_candle_data.as_ref() {
            data
                .get_candles(&self.trading_config.frequency)
                .map_err(|e| BacktestingErrors::CandleError(e))
        } else {
            Err(BacktestingErrors::APIError("`trading_candle_data` is None".to_string()))
        }
    }

    fn get_market_asset(&self) -> Result<&DataFrame, BacktestingErrors> {
        if let Some(data) = self.market_candle_data.as_ref() {
            data
                .get_candles(&self.trading_config.frequency)
                .map_err(|e| BacktestingErrors::CandleError(e))
        } else {
            Err(BacktestingErrors::APIError("`market_candle_data` is None".to_string()))
        }
    }

    /// Run the backtesting simulation
    pub fn run(&mut self) -> Result<(), BacktestingErrors> {
        // ensure that candles are set
        if self.trading_candles.is_none() || self.market_candles.is_none() {
            return Err(BacktestingErrors::APIError("Candle data is None".to_string()));
        }

        // ensure that the market data and historical data are sorted by timestamp
        info!("Checking candle data and market data alignment");
        let _ = check_candle_alignment(self.trading_candles.as_ref().unwrap(), self.market_candles.as_ref().unwrap())
            .map_err(|e| BacktestingErrors::AlignmentError(e));

        let mut portfolio = self.initialize_portfolio()?;

        // initialize position manager
        let mut position_manager = PositionManager::new(self.manager_config.clone());

        let candle_rows = extract_candles_from_df(self.trading_candles.as_ref().unwrap()).unwrap();

        // begin trading simulation
        let start_time = Instant::now();
        for candle in candle_rows {
            let trimmed_trading_candles = trim_candles(self.trading_candles.as_ref().unwrap(), candle.time, CANDLE_TRIM_SIZE);
            if trimmed_trading_candles.height() == 0 {
                continue;
            }
            let signal = self.strategy.process_candle(&trimmed_trading_candles)
                .map_err(|_| BacktestingErrors::SignalExtractionError)?;

            let trimmed_candles = extract_candles_from_df(&trimmed_trading_candles).unwrap();

            // trim market data
            let trimmed_market = trim_candles(&self.market_candles.as_ref().unwrap(), candle.time, CANDLE_TRIM_SIZE);
            let trimmed_market = extract_candles_from_df(&trimmed_market).unwrap();

            // calculate current portfolio risk metrics
            let risk = calculate_risk(&portfolio, &trimmed_market, &trimmed_candles)
                .map_err(|e| {
                    info!("Error calculating risk: {:?}", e);
                    BacktestingErrors::RiskCalculationError(e)
                })?;

            let current_price = candle.close;

            // make decision based on risk, signals and current market conditions
            let decision = position_manager.make_decision(&mut portfolio, &risk, &signal, current_price)
                .map_err(|e| {
                    info!("Error making decision: {:?}", e);
                    BacktestingErrors::DecisionError(e)
                })?;

            let trade = match decision {
                TradeDecision::ExecuteBuy(quantity) => {
                    FutureTrade::new(Side::Buy, current_price, quantity, candle.time)
                },
                TradeDecision::ExecuteSell(quantity, trade_ids) => {
                    info!("Closing positions: {:?}", trade_ids);
                    FutureTrade::new(Side::Sell, current_price, quantity, candle.time)
                },
                TradeDecision::DoNothing => continue,
            };

            // attempt trades
            // TODO: simulate market conditions by adding randomness
            let executed = ExecutedTrade::from_future_trade(candle.time.to_string(), trade);
            portfolio.add_executed_trade(executed);
        }
        let elapsed = start_time.elapsed();

        self.print_statistics(elapsed, &portfolio);

        Ok(())
    }

    /// Create a portfolio from the [`PortfolioArgs`]
    ///
    /// # Arguments
    /// * `candles` - The historical candles used to initialize the portfolio. Used to extract the starting time.
    fn initialize_portfolio(&self) -> Result<Portfolio, BacktestingErrors> {
        if let Some(candles) = self.trading_candles.as_ref() {
            let start_time = candles.column("time").unwrap().datetime().unwrap().get(0).unwrap();
            let start_time = DateTime::from_timestamp_millis(start_time).unwrap().naive_utc();
            Ok(Portfolio::from_args(&self.portfolio_args, start_time))
        } else {
            Err(BacktestingErrors::APIError("Trading candles are None".to_string()))
        }
    }

    /// Print statistics about the backtesting run
    ///
    /// # Arguments
    /// * `candles` - Only candle length is used, so any candle [`DataFrame`] can be passed.
    /// * `duration` - The duration of the backtesting run
    /// * `portfolio` - The portfolio after the backtesting run
    fn print_statistics(&self, duration: Duration, portfolio: &Portfolio) {
        // print basic statistics
        print_portfolio(portfolio, self.portfolio_args.capital);

        let candles = self.trading_candles.as_ref().unwrap();

        print_candle_statistics(candles);

        let candle_len = candles.height();
        info!(r#"Finished processing {:?} rows in {:?}
Avg. processing time per row: {:?}"#,
            candle_len,
            duration,
            duration / candle_len as u32);
    }

    /// Save candles and indicators as CSV
    ///
    /// # Arguments
    /// * `path` - The directory to save the data
    pub fn save_data<P: Into<PathBuf>>(&mut self, path: P) {
        let path = path.into();

        // check that the path is not a file, and exists
        if path.is_file() {
            panic!("Path is a file, expected a directory");
        }
        else if !path.exists() {
            std::fs::create_dir(&path).unwrap();
        }

        // save trading assets
        let filename = format!("{}_{}.csv", self.trading_config.trading_asset, self.trading_config.frequency);
        let trading_candles_path = path.join(filename);
        save_candles(self.trading_candles.as_mut().unwrap(), trading_candles_path.to_str().unwrap()).unwrap();

        // save market data
        let filename = format!("{}_{}.csv", self.trading_config.market_asset, self.trading_config.frequency);
        let market_candles_path = path.join(filename);
        save_candles(self.market_candles.as_mut().unwrap(), market_candles_path.to_str().unwrap()).unwrap();

        // save indicators
        self.strategy.save_indicators(self.trading_candles.as_ref().unwrap(), path);
    }
}

fn print_portfolio(portfolio: &Portfolio, starting_capital: Decimal) {

    info!(r#"Number of open positions: {}
Total open quantity: {}
Total open value: {}
Total executed positions: {}
Profit: {}"#,
        portfolio.get_open_positions().len(),
        portfolio.total_open_quantity(),
        portfolio.total_position_value(),
        portfolio.get_executed_trades().len(),
        portfolio.available_capital() - starting_capital);
}
