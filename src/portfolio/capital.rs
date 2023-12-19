use chrono::NaiveDateTime;
use crate::portfolio::Portfolio;

/// Interface methods for interacting with the total capital available to a portfolio.
/// 
/// These are wrapper functions for interacting with the underlying `TrackedValue` struct.
pub trait CapitalHandlers {
    fn increase_capital<T>(&mut self, amount: f64, point: T)
        where T: Into<Option<NaiveDateTime>>;
    fn decrease_capital<T>(&mut self, amount: f64, point: T)
        where T: Into<Option<NaiveDateTime>>;
    fn get_capital(&self) -> f64;
}

impl CapitalHandlers for Portfolio {
    fn increase_capital<T>(&mut self, amount: f64, point: T)
        where T: Into<Option<NaiveDateTime>> {
        self.capital_ts.increment(amount, point);
    }

    fn decrease_capital<T>(&mut self, amount: f64, point: T)
        where T: Into<Option<NaiveDateTime>> {
        self.capital_ts.decrement(amount, point);
    }

    fn get_capital(&self) -> f64 {
        self.capital_ts.get_last_value()
    }
}


#[cfg(test)]
mod tests {
    #[test]
    fn test_increase_capital() {
        use super::*;

        let mut portfolio = Portfolio::new(100.0, 100.0, None);
        portfolio.increase_capital(10.0, None);
        assert_eq!(portfolio.get_capital(), 110.0);
    }

    #[test]
    fn test_decrease_capital() {
        use super::*;

        let mut portfolio = Portfolio::new(100.0, 100.0, None);
        portfolio.decrease_capital(10.0, None);
        assert_eq!(portfolio.get_capital(), 90.0);
    }
    
    #[test]
    fn test_get_capital() {
        use super::*;

        let mut portfolio = Portfolio::new(100.0, 100.0, None);
        assert_eq!(portfolio.get_capital(), 100.0);
        
        portfolio.increase_capital(10.0, None);
        assert_eq!(portfolio.get_capital(), 110.0);
        
        portfolio.decrease_capital(10.0, None);
        assert_eq!(portfolio.get_capital(), 100.0);
    }
}