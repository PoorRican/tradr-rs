use crate::portfolio::Portfolio;
use chrono::NaiveDateTime;
use rust_decimal::Decimal;

/// Interface methods for interacting with the total capital available to a portfolio.
///
/// These are wrapper functions for interacting with the underlying `TrackedValue` struct.
pub trait CapitalHandlers {
    fn increase_capital<T>(&mut self, amount: Decimal, point: T)
    where
        T: Into<Option<NaiveDateTime>>;
    fn decrease_capital<T>(&mut self, amount: Decimal, point: T)
    where
        T: Into<Option<NaiveDateTime>>;
    fn available_capital(&self) -> Decimal;
}

impl CapitalHandlers for Portfolio {
    fn increase_capital<T>(&mut self, amount: Decimal, point: T)
    where
        T: Into<Option<NaiveDateTime>>,
    {
        self.capital_ts.increment(amount, point);
    }

    fn decrease_capital<T>(&mut self, amount: Decimal, point: T)
    where
        T: Into<Option<NaiveDateTime>>,
    {
        self.capital_ts.decrement(amount, point);
    }

    fn available_capital(&self) -> Decimal {
        self.capital_ts.get_last_value()
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    #[test]
    fn test_increase_capital() {
        use super::*;

        let mut portfolio = Portfolio::new(dec!(100.0), dec!(100.0), None);
        portfolio.increase_capital(dec!(10.0), None);
        assert_eq!(portfolio.available_capital(), dec!(110.0));
    }

    #[test]
    fn test_decrease_capital() {
        use super::*;

        let mut portfolio = Portfolio::new(dec!(100.0), dec!(100.0), None);
        portfolio.decrease_capital(dec!(10.0), None);
        assert_eq!(portfolio.available_capital(), dec!(90.0));
    }

    #[test]
    fn test_get_capital() {
        use super::*;

        let mut portfolio = Portfolio::new(dec!(100.0), dec!(100.0), None);
        assert_eq!(portfolio.available_capital(), dec!(100.0));

        portfolio.increase_capital(dec!(10.0), None);
        assert_eq!(portfolio.available_capital(), dec!(110.0));

        portfolio.decrease_capital(dec!(10.0), None);
        assert_eq!(portfolio.available_capital(), dec!(100.0));
    }
}
