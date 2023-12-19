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
}
