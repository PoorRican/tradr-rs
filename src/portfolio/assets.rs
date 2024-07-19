use crate::portfolio::Portfolio;
use chrono::NaiveDateTime;
use rust_decimal::Decimal;

/// Interface methods for interacting with the total assets available to a portfolio.
///
/// These are wrapper functions for interacting with the underlying `TrackedValue` struct.
pub trait AssetHandlers {
    fn increase_assets<T>(&mut self, amount: Decimal, point: T)
    where
        T: Into<Option<NaiveDateTime>>;
    fn decrease_assets<T>(&mut self, amount: Decimal, point: T)
    where
        T: Into<Option<NaiveDateTime>>;
    fn get_assets(&self) -> Decimal;
}

impl AssetHandlers for Portfolio {
    /// Increase assets by the given amount
    fn increase_assets<T>(&mut self, amount: Decimal, point: T)
    where
        T: Into<Option<NaiveDateTime>>,
    {
        self.assets_ts.increment(amount, point);
    }

    /// Decrease assets by the given amount
    fn decrease_assets<T>(&mut self, amount: Decimal, point: T)
    where
        T: Into<Option<NaiveDateTime>>,
    {
        self.assets_ts.decrement(amount, point);
    }

    /// Get the current amount of assets
    fn get_assets(&self) -> Decimal {
        self.assets_ts.get_last_value()
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    #[test]
    fn test_increase_assets() {
        use super::*;

        let mut portfolio = Portfolio::new(dec!(100.0), dec!(100.0), None);
        portfolio.increase_assets(dec!(10.0), None);
        assert_eq!(portfolio.get_assets(), dec!(110.0));
    }

    #[test]
    fn test_decrease_assets() {
        use super::*;

        let mut portfolio = Portfolio::new(dec!(100.0), dec!(100.0), None);
        portfolio.decrease_assets(dec!(10.0), None);
        assert_eq!(portfolio.get_assets(), dec!(90.0));
    }

    #[test]
    fn test_get_assets() {
        use super::*;

        let mut portfolio = Portfolio::new(dec!(100.0), dec!(100.0), None);
        assert_eq!(portfolio.get_assets(), dec!(100.0));

        portfolio.increase_assets(dec!(10.0), None);
        assert_eq!(portfolio.get_assets(), dec!(110.0));

        portfolio.decrease_assets(dec!(100.0), None);
        assert_eq!(portfolio.get_assets(), dec!(10.0));
    }
}
