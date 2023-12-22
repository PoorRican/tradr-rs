use chrono::NaiveDateTime;
use polars::frame::DataFrame;
use crate::portfolio::Portfolio;
use crate::types::FutureTrade;

pub trait Strategy {
    /// Returns a reference to the internally stored portfolio
    fn get_portfolio(&self) -> &Portfolio;

    /// Builder function to add a reference to a portfolio
    fn add_portfolio(self, portfolio: &Portfolio) -> Self;

    /// Builder function to add a reference to candles
    fn add_candles(self, candles: &DataFrame) -> Self;

    /// Generate a trade to attempt to execute on the market
    fn process(&mut self, point: Option<NaiveDateTime>) -> Option<FutureTrade>;
}
