use chrono::{DateTime, Utc};
use crate::types::portfolio::Portfolio;
use crate::types::trades::future::FutureTrade;

pub trait Strategy {
    fn get_order_handler(&self) -> &Portfolio;
    fn build_order_handler(&mut self, order_handler: Portfolio) -> &mut Self;

    /// Generate a trade to attempt to execute on the market
    fn process(&mut self, point: Option<DateTime<Utc>>) -> Option<FutureTrade>;

}
