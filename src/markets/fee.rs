use crate::types::Side;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

/// A trait for calculating the amounts of fees to be deducted from a trade.
///
/// Since most exchanges have different fee structures, this trait is used to
/// abstract away the fee calculation logic. The `cost_with_fee_deducted()` method
/// is used to calculate the cost of a trade after the fee has been deducted.
///
/// For buy trades, the fee is added to the cost of the buy order. For sell trades, the fee is
/// subtracted from the amount of quote currency yielded by the trade.
pub trait FeeCalculator {
    fn cost_including_fee(&self, cost: Decimal, side: Side) -> Decimal;
}

/// A simple fee calculator that has a fixed percentage fee.
///
/// This fee calculator is used to calculate the amount of currency that a buy order will cost
/// or the amount of currency that a sell order will yield. The fee is calculated as a percentage
/// of the cost of the trade.
///
/// Therefore, for a buy order, the fee is added to the cost of the trade. For a sell order, the
/// fee is subtracted from the amount of quote currency yielded by the trade.
///
/// This fee calculator assumes that the fee is the same for both buy and sell orders.
pub struct SimplePercentageFee {
    taker_fee: Decimal,
}

impl SimplePercentageFee {
    pub fn new(fee_percentage: Decimal) -> Self {
        Self {
            taker_fee: fee_percentage / dec!(100.0),
        }
    }
}

impl FeeCalculator for SimplePercentageFee {
    fn cost_including_fee(&self, cost: Decimal, side: Side) -> Decimal {
        let fee = cost * self.taker_fee;
        match side {
            Side::Buy => cost + fee,
            Side::Sell => cost - fee,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_percentage_taker_fee_calculator() {
        let trade_price = dec!(100.0);
        let fee_calculator = SimplePercentageFee::new(dec!(0.8));

        // assert that the fee calculator was initialized correctly
        assert_eq!(fee_calculator.taker_fee, dec!(0.008));

        // assert that the fee for a buy trade is calculated correctly
        let fee = fee_calculator.cost_including_fee(trade_price, Side::Buy);
        assert_eq!(fee, dec!(100.8));

        // assert that the fee for a sell trade is calculated correctly
        let fee = fee_calculator.cost_including_fee(trade_price, Side::Sell);
        assert_eq!(fee, dec!(99.2));
    }
}
