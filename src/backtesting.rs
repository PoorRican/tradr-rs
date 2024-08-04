use std::path::Path;
use std::time::{Instant, Duration};
use chrono::{DateTime};
use log::info;
use polars::prelude::*;
use serde::Deserialize;
use crate::manager::{PositionManager, PositionManagerConfig, PositionManagerError, TradeDecision};
use crate::markets::utils::save_candles;
use crate::portfolio::{Portfolio, PortfolioArgs, PositionHandlers, TradeHandlers};
use crate::processor::CandleProcessor;
use crate::risk::{calculate_risk, RiskCalculationErrors};
use crate::strategies::Strategy;
use crate::types::{Candle, ExecutedTrade, FutureTrade, MarketData, MarketDataError, Side, Signal};
use crate::utils;
use crate::utils::{AlignmentError, check_candle_alignment, print_candle_statistics, trim_candles};

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
        // load candle data
        self.market_candle_data = MarketData::from_db(&self.trading_config.market_asset).into();
        self.trading_candle_data = MarketData::from_db(&self.trading_config.trading_asset).into();

        // compute indicator graph
        let trading_candles = self.get_trading_asset()?.to_owned();

        self.strategy.process_historical_candles(&trading_candles).unwrap();

        // populate market and trading candles
        self.trading_candles = trading_candles.into();
        self.market_candles = self.get_market_asset()?.to_owned().into();

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

        // process historical data
        let (relevant_candles, signals) = self.get_relevant_signals()?;

        // begin trading simulation
        let start_time = Instant::now();
        for (candle, signal) in relevant_candles.iter().zip(signals.iter()) {
            let trimmed_trading_candles = trim_candles(self.trading_candles.as_ref().unwrap(), candle.time, CANDLE_TRIM_SIZE);
            if trimmed_trading_candles.height() == 0 {
                continue;
            }
            let trimmed_candles = utils::extract_candles_from_df(&trimmed_trading_candles).unwrap();

            // trim market data
            let trimmed_market = trim_candles(&self.market_candles.as_ref().unwrap(), candle.time, CANDLE_TRIM_SIZE);
            let trimmed_market = utils::extract_candles_from_df(&trimmed_market).unwrap();

            // calculate current portfolio risk metrics
            let risk = calculate_risk(&portfolio, &trimmed_market, &trimmed_candles)
                .map_err(|e| {
                    info!("Error calculating risk: {:?}", e);
                    BacktestingErrors::RiskCalculationError(e)
                })?;

            let current_price = candle.close;

            // make decision based on risk, signals and current market conditions
            let decision = position_manager.make_decision(&mut portfolio, &risk, signal, current_price)
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

        self.save_data();

        Ok(())
    }

    /// Write all indicator graphs to a specific dir
    ///
    /// # Arguments
    /// * `dir` - The path to save the indicator graph files
    pub fn save_indicator_data(&mut self, dir: &str) -> Result<(), PolarsError> {
        let dir_path = Path::new(dir);

        for indicator in self.strategy.indicators.iter_mut() {
            let indicator = indicator.as_mut();
            let file_name = format!("{}_graph.csv", indicator.get_name());
            let path = dir_path.join(file_name);
            let path = path.to_str().unwrap();
            indicator.save_graph_as_csv(path)?;
        }
        Ok(())
    }

    /// This is used to run the backtesting simulation on rows which are relevant.
    ///
    /// Generate relevant rows signals and return values from which to iterate over
    ///
    /// Signals are generated for the current candles, the candles are joined with the signals.
    /// A combined iterator of signals which are not `Hold` and candles is returned.
    ///
    ///
    /// # Arguments
    /// * `candles` - Candles to use for signal generation
    fn get_relevant_signals(&mut self) -> Result<(Vec<Candle>, Vec<Signal>), BacktestingErrors> {
        // generate signals
        let signals =  self.strategy.get_combined_signals()
            .map_err(|_| BacktestingErrors::SignalExtractionError)?;
        let signals = signals.unwrap();

        // join signals column with candles df
        let combined = self.trading_candles
            .clone()
            .unwrap()
            .lazy()
            .join(
                signals.lazy(),
                [col("time")],
                [col("time")],
                JoinArgs::new(JoinType::Left),
            )
            .collect().unwrap();

        // ensure that columns are correct
        assert_eq!(combined.get_column_names(), &["time", "open", "high", "low", "close", "volume", "signals"]);

        // convert to vector of candles and vector of signals
        let candles_vec = utils::extract_candles_from_df(&combined).unwrap();
        let sides: Vec<Signal> = utils::extract_signals_from_df(&combined, "signals").unwrap();

        Ok((candles_vec, sides))
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
        print_portfolio(portfolio);

        let candles = self.trading_candles.as_ref().unwrap();

        print_candle_statistics(candles);

        let candle_len = candles.height();
        info!("Finished processing {:?} rows in {:?}", candle_len, duration);
        info!("Avg. processing time per row: {:?}", duration / candle_len as u32);
    }

    fn save_data(&mut self) {
        // save trading assets
        let path = format!("data/{}_{}.csv", self.trading_config.trading_asset, self.trading_config.frequency);
        save_candles(self.trading_candles.as_mut().unwrap(), &path).unwrap();

        // save market data
        let path = format!("data/{}_{}.csv", self.trading_config.market_asset, self.trading_config.frequency);
        save_candles(self.market_candles.as_mut().unwrap(), &path).unwrap();

        // save strategy indicator data
        self.save_indicator_data("data").unwrap()
    }
}

fn print_portfolio(portfolio: &Portfolio) {
    info!("Number of open positions: {}", portfolio.get_open_positions().len());
    info!("Total open quantity: {}", portfolio.total_open_quantity());
    info!("Total open value: {}", portfolio.total_position_value());
    info!("Total positions: {}", portfolio.get_executed_trades().len());
}
