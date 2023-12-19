mod tracked;
pub mod assets;
pub mod capital;
pub mod trade;
pub mod position;

use chrono::{Duration, NaiveDateTime, Utc};
use polars::prelude::DataFrame;
use crate::portfolio::tracked::TrackedValue;

const DEFAULT_LIMIT: usize = 4;
const DEFAULT_TIMEOUT_MINUTES: i64 = 60 * 2;
const DEFAULT_THRESHOLD: f64 = 0.50;

pub struct Portfolio {
    failed_trades: DataFrame,
    executed_trades: DataFrame,
    open_positions: Vec<NaiveDateTime>,

    threshold: f64,
    assets_ts: TrackedValue,
    capital_ts: TrackedValue,
    open_positions_limit: usize,
    timeout: Duration,
}

impl Portfolio {
    pub fn new<T>(assets: f64, capital: f64, point: T) -> Portfolio
    where T: Into<Option<NaiveDateTime>> {
        let point = point.into()
            .unwrap_or_else(
                || NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap());

        Portfolio {
            failed_trades: DataFrame::empty(),
            executed_trades: DataFrame::empty(),
            open_positions: vec![],

            threshold: DEFAULT_THRESHOLD,
            assets_ts: TrackedValue::with_initial(assets, point),
            capital_ts: TrackedValue::with_initial(capital, point),
            open_positions_limit: DEFAULT_LIMIT,
            timeout: Duration::minutes(DEFAULT_TIMEOUT_MINUTES),
        }
    }

    /// Setter for the profitability threshold parameter
    ///
    /// # Arguments
    /// * `threshold` - The new profitability threshold in unit currency
    pub fn set_threshold(&mut self, threshold: f64) {
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
    use crate::portfolio::{
        assets::AssetHandlers,
        capital::CapitalHandlers
    };

    #[test]
    fn test_new() {
        use chrono::NaiveDateTime;

        let assets = 100.0;
        let capital = 100.0;
        let point = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap();

        let portfolio = Portfolio::new(assets, capital, point);

        // assert that assets and capital `TrackedValues` are initialized correctly
        assert_eq!(portfolio.get_assets(), assets);
        assert_eq!(portfolio.get_capital(), capital);

        // assert that the default parameters are set correctly
        assert_eq!(portfolio.threshold, DEFAULT_THRESHOLD);
        assert_eq!(portfolio.open_positions_limit, DEFAULT_LIMIT);
        assert_eq!(portfolio.timeout, Duration::minutes(DEFAULT_TIMEOUT_MINUTES));

        // assert that the trade storage is empty
        assert!(portfolio.failed_trades.is_empty());
        assert!(portfolio.executed_trades.is_empty());
        assert!(portfolio.open_positions.is_empty());
    }

    #[test]
    fn test_set_threshold() {
        let mut portfolio = Portfolio::new(100.0, 100.0, None);
        assert_eq!(portfolio.threshold, DEFAULT_THRESHOLD);

        portfolio.set_threshold(0.25);
        assert_eq!(portfolio.threshold, 0.25);
    }

    #[test]
    fn test_set_open_positions_limit() {
        let mut portfolio = Portfolio::new(100.0, 100.0, None);
        assert_eq!(portfolio.open_positions_limit, DEFAULT_LIMIT);

        portfolio.set_open_positions_limit(2);
        assert_eq!(portfolio.open_positions_limit, 2);
    }

    #[test]
    fn test_set_timeout() {
        let mut portfolio = Portfolio::new(100.0, 100.0, None);
        assert_eq!(portfolio.timeout, Duration::minutes(DEFAULT_TIMEOUT_MINUTES));

        portfolio.set_timeout(10);
        assert_eq!(portfolio.timeout, Duration::minutes(10));
    }
}