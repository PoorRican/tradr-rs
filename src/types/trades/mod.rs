mod executed;
mod failed;
mod future;

pub use executed::ExecutedTrade;
pub use failed::FailedTrade;
pub use future::FutureTrade;

use crate::types::signals::Side;
use chrono::NaiveDateTime;
use rust_decimal::Decimal;

pub trait Trade {
    fn get_side(&self) -> Side;

    /// Get the price/rate of the traded asset
    fn get_price(&self) -> Decimal;

    /// Get the quantity of the traded asset
    fn get_quantity(&self) -> Decimal;

    /// Get the total cost of the trade
    fn get_notional_value(&self) -> Decimal;

    fn get_timestamp(&self) -> &NaiveDateTime;
}

pub fn calc_notional_value(price: Decimal, quantity: Decimal) -> Decimal {
    price * quantity
}
