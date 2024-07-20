mod assets;
mod capital;
mod position;
mod tracked;
mod trade;

use std::collections::HashMap;
pub use assets::AssetHandlers;
pub use capital::CapitalHandlers;
pub use position::PositionHandlers;
pub use trade::TradeHandlers;

use crate::markets::FeeCalculator;
use crate::portfolio::tracked::TrackedValue;
use chrono::{Duration, NaiveDateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use crate::types::{ExecutedTrade, FailedTrade};

pub const DEFAULT_LIMIT: usize = 4;
pub const DEFAULT_TIMEOUT_MINUTES: i64 = 60 * 2;
pub const DEFAULT_THRESHOLD: Decimal = dec!(0.5);

/// Arguments for creating a new portfolio via the [`Portfolio::from_args`] constructor
///
/// This is used in backtesting to dynamically creating a [`Portfolio`] with the desired parameters when the
/// start time (used for internal tracking) is not known.
///
/// # Examples
///
/// All configurable parameters are accessible via the fields of the struct.
///
/// ```
/// use crate::portfolio::{PortfolioArgs, Portfolio};
///
/// let args = PortfolioArgs {
///    assets: 0.0,
///    capital: 100.0,
///    threshold: 0.25,
///    open_positions_limit: 2,
///    timeout: 60 * 2,
/// };
///
/// // create a new Portfolio using the `from_args` constructor
/// let portfolio = Portfolio::from_args(&args, NaiveDateTime::from_timestamp(0, 0));
/// ```
///
/// Any value that is not provided will default to the value specified in the [`Default`] implementation.
/// ```
/// use crate::portfolio::{PortfolioArgs, Portfolio};
///
/// let args = PortfolioArgs {
///    assets: 0.0,
///    capital: 100.0,
///   ..Default::default()
/// };
///
/// let portfolio = Portfolio::from_args(&args, NaiveDateTime::from_timestamp(0, 0));
/// ```
pub struct PortfolioArgs {
    pub assets: Decimal,
    pub capital: Decimal,
    pub threshold: Decimal,
    pub open_positions_limit: usize,
    pub timeout: i64,
}
impl Default for PortfolioArgs {
    fn default() -> Self {
        PortfolioArgs {
            assets: dec!(0.0),
            capital: dec!(100.0),
            threshold: DEFAULT_THRESHOLD,
            open_positions_limit: DEFAULT_LIMIT,
            timeout: DEFAULT_TIMEOUT_MINUTES,
        }
    }
}

/// This struct is used to manage an entire portfolio for a given asset.
///
/// It is responsible for managing the assets and capital available to the portfolio,
/// as well as the open positions and executed trades.
pub struct Portfolio {
    failed_trades: Vec<FailedTrade>,
    executed_trades: HashMap<NaiveDateTime, ExecutedTrade>,
    open_positions: Vec<NaiveDateTime>,

    threshold: Decimal,
    assets_ts: TrackedValue,
    capital_ts: TrackedValue,
    open_positions_limit: usize,
    timeout: Duration,

    fee_calculator: Option<Box<dyn FeeCalculator>>,
}

impl Default for Portfolio {
    fn default() -> Self {
        Self {
            failed_trades: vec![],
            executed_trades: HashMap::new(),
            open_positions: vec![],

            threshold: DEFAULT_THRESHOLD,
            assets_ts: TrackedValue::default(),
            capital_ts: TrackedValue::default(),
            open_positions_limit: DEFAULT_LIMIT,
            timeout: Duration::minutes(DEFAULT_TIMEOUT_MINUTES),
            fee_calculator: None,
        }
    }

}

impl Portfolio {
    pub fn new<T>(assets: Decimal, capital: Decimal, timestamp: T) -> Portfolio
    where
        T: Into<Option<NaiveDateTime>>,
    {
        let point = timestamp.into().unwrap_or_else(|| {
            NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap()
        });

        Portfolio {
            assets_ts: TrackedValue::with_initial(assets, point),
            capital_ts: TrackedValue::with_initial(capital, point),
            ..Default::default()
        }
    }

    pub fn from_args(args: &PortfolioArgs, start_time: NaiveDateTime) -> Self {
        Self {
            threshold: args.threshold,
            assets_ts: TrackedValue::with_initial(args.assets, start_time),
            capital_ts: TrackedValue::with_initial(args.capital, start_time),
            open_positions_limit: args.open_positions_limit,
            timeout: Duration::minutes(args.timeout),
            fee_calculator: None,
            ..Default::default()
        }
    }

    /// Constructor with loaded data
    pub fn with_data(
        failed_trades: Vec<FailedTrade>,
        executed_trades: HashMap<NaiveDateTime, ExecutedTrade>,
        open_positions: Vec<NaiveDateTime>,
        assets_ts: TrackedValue,
        capital_ts: TrackedValue,
    ) -> Portfolio {
        Portfolio {
            failed_trades,
            executed_trades,
            open_positions,
            threshold: DEFAULT_THRESHOLD,
            assets_ts,
            capital_ts,
            open_positions_limit: DEFAULT_LIMIT,
            timeout: Duration::minutes(DEFAULT_TIMEOUT_MINUTES),
            fee_calculator: None,
        }
    }

    /// Builder method for the `fee_calculator` field
    pub fn add_fee_calculator<T>(mut self, fee_calculator: T) -> Self
    where
        T: FeeCalculator + 'static,
    {
        self.fee_calculator = Some(Box::new(fee_calculator));
        self
    }

    /// Setter for the profitability threshold parameter
    ///
    /// # Arguments
    /// * `threshold` - The new profitability threshold in unit currency
    pub fn set_threshold(&mut self, threshold: Decimal) {
        self.threshold = threshold;
    }

    /// Setter for the open positions limit parameter
    ///
    /// This is used by `Portfolio::available_open_positions()` to determine the number of
    /// available open positions at any given time.
    ///
    /// # Arguments
    /// * `limit` - The number of open positions allowed at any given time
    pub fn set_open_positions_limit(&mut self, limit: usize) {
        self.open_positions_limit = limit;
    }

    /// Setter for the open positions timeout parameter
    ///
    /// # Arguments
    /// * `minute` - The number of minutes after which an open position is closed
    pub fn set_timeout(&mut self, minute: usize) {
        self.timeout = Duration::minutes(minute as i64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::portfolio::{assets::AssetHandlers, capital::CapitalHandlers};
    use crate::types::{ExecutedTrade, FailedTrade, FutureTrade, ReasonCode, Side};
    #[test]
    fn test_with_data() {
        use crate::types::Side;
        use chrono::NaiveDateTime;

        let assets = dec!(100.0);
        let capital = dec!(100.0);
        let point = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap();

        let mut portfolio = Portfolio::new(assets, capital, point);
        let trade = FutureTrade::new(Side::Buy, dec!(100.0), dec!(1.0), point + Duration::seconds(1));
        let executed_trade = ExecutedTrade::from_future_trade("id".to_string(), trade.clone());
        let failed_trade =
            FailedTrade::with_future_trade(ReasonCode::MarketRejection, trade.clone());

        portfolio.add_executed_trade(executed_trade);
        portfolio.add_failed_trade(failed_trade);

        let portfolio = Portfolio::with_data(
            portfolio.failed_trades,
            portfolio.executed_trades,
            portfolio.open_positions,
            portfolio.assets_ts,
            portfolio.capital_ts,
        );

        // assert that assets and capital `TrackedValues` were initialized correctly
        assert_eq!(portfolio.get_assets(), assets + dec!(1.0));
        assert_eq!(portfolio.available_capital(), capital - dec!(100.0));

        // assert that the default parameters are set correctly
        assert_eq!(portfolio.threshold, DEFAULT_THRESHOLD);
        assert_eq!(portfolio.open_positions_limit, DEFAULT_LIMIT);
        assert_eq!(
            portfolio.timeout,
            Duration::minutes(DEFAULT_TIMEOUT_MINUTES)
        );

        // assert that the trade storage is empty
        assert_eq!(portfolio.executed_trades.len(), 1);
        assert_eq!(portfolio.failed_trades.len(), 1);
        assert_eq!(portfolio.open_positions.len(), 1);
    }

    #[test]
    fn test_new() {
        use chrono::NaiveDateTime;

        let assets = dec!(100.0);
        let capital = dec!(100.0);
        let point = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap();

        let portfolio = Portfolio::new(assets, capital, point);

        // assert that assets and capital `TrackedValues` are initialized correctly
        assert_eq!(portfolio.get_assets(), assets);
        assert_eq!(portfolio.available_capital(), capital);

        // assert that the default parameters are set correctly
        assert_eq!(portfolio.threshold, DEFAULT_THRESHOLD);
        assert_eq!(portfolio.open_positions_limit, DEFAULT_LIMIT);
        assert_eq!(
            portfolio.timeout,
            Duration::minutes(DEFAULT_TIMEOUT_MINUTES)
        );

        // assert that the trade storage is empty
        assert!(portfolio.failed_trades.is_empty());
        assert!(portfolio.executed_trades.is_empty());
        assert!(portfolio.open_positions.is_empty());
    }

    #[test]
    fn test_add_fee_calculator() {
        use crate::markets::SimplePercentageFee;
        let portfolio = Portfolio::new(dec!(100.0), dec!(100.0), None);
        assert!(portfolio.fee_calculator.is_none());

        let portfolio = portfolio.add_fee_calculator(SimplePercentageFee::new(dec!(0.8)));
        assert!(portfolio.fee_calculator.is_some());
    }

    #[test]
    fn test_set_threshold() {
        let mut portfolio = Portfolio::new(dec!(100.0), dec!(100.0), None);
        assert_eq!(portfolio.threshold, DEFAULT_THRESHOLD);

        portfolio.set_threshold(dec!(0.25));
        assert_eq!(portfolio.threshold, dec!(0.25));
    }

    #[test]
    fn test_set_open_positions_limit() {
        let mut portfolio = Portfolio::new(dec!(100.0), dec!(100.0), None);
        assert_eq!(portfolio.open_positions_limit, DEFAULT_LIMIT);

        portfolio.set_open_positions_limit(2);
        assert_eq!(portfolio.open_positions_limit, 2);
    }

    #[test]
    fn test_set_timeout() {
        let mut portfolio = Portfolio::new(dec!(100.0), dec!(100.0), None);
        assert_eq!(
            portfolio.timeout,
            Duration::minutes(DEFAULT_TIMEOUT_MINUTES)
        );

        portfolio.set_timeout(10);
        assert_eq!(portfolio.timeout, Duration::minutes(10));
    }

}
