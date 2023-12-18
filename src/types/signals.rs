const BUY: isize = 1;
const SELL: isize = -1;

#[derive(Debug, Clone, Copy, PartialEq)]
/// Abstracts indicator outputs
pub enum Signal {
    Sell = SELL,
    Hold = 0,
    Buy = BUY,
}

#[derive(Debug, Clone, Copy, PartialEq)]
/// Abstracts types of trades
pub enum Side {
    Sell = SELL,
    Buy = BUY,
}
