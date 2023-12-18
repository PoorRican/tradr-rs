mod ts_value;

use chrono::{Duration, NaiveDateTime};
use polars::prelude::DataFrame;
use crate::types::trades::failed::FailedTrade;
use crate::types::trades::executed::ExecutedTrade;
use crate::traits::AsDataFrame;

pub struct Portfolio {
    failed_trades: DataFrame,
    executed_trades: DataFrame,
    open_positions: Vec<NaiveDateTime>,

    threshold: f64,
    assets_ts: DataFrame,
    capital_ts: DataFrame,
    open_positions_limit: usize,
    timeout: Duration,
}

impl Portfolio {
    pub fn add_failed_trade(&mut self, trade: FailedTrade) {
        let row = trade.as_dataframe();
        self.failed_trades = self.failed_trades.vstack(&row).unwrap();
    }

    pub fn add_executed_trade(&mut self, trade: ExecutedTrade) {
        let row = trade.as_dataframe();
        self.executed_trades = self.executed_trades.vstack(&row).unwrap();
    }

    pub fn set_as_open_position(&mut self) {
        // TODO: put last buy in open_position
    }

    pub fn get_open_positions(&self) -> Option<DataFrame> {
        // TODO: return all rows from executed_trades
        None
    }
}
