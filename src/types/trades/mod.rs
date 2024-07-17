mod executed;
mod failed;
mod future;

pub use executed::ExecutedTrade;
pub use failed::FailedTrade;
pub use future::FutureTrade;

use crate::traits::AsDataFrame;
use crate::types::signals::Side;
use chrono::NaiveDateTime;

pub trait Trade: AsDataFrame {
    fn get_side(&self) -> Side;
    /// Get the price/rate of the traded asset
    fn get_price(&self) -> f64;
    /// Get the quantity of the traded asset
    fn get_quantity(&self) -> f64;
    /// Get the total cost of the trade
    fn get_cost(&self) -> f64;

    fn get_point(&self) -> &NaiveDateTime;
}

// TODO: truncate (floor) to 2 decimal places
pub fn calc_cost(price: f64, quantity: f64) -> f64 {
    price * quantity
}
