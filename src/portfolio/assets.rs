use chrono::NaiveDateTime;
use crate::portfolio::Portfolio;

/// Interface methods for interacting with the total assets available to a portfolio.
/// 
/// These are wrapper functions for interacting with the underlying `TrackedValue` struct.
pub trait AssetHandlers {
    fn increase_assets<T>(&mut self, amount: f64, point: T)
    where T: Into<Option<NaiveDateTime>>;
    fn decrease_assets<T>(&mut self, amount: f64, point: T)
    where T: Into<Option<NaiveDateTime>>;
    fn get_assets(&self) -> f64;
}

impl AssetHandlers for Portfolio {
    /// Increase assets by the given amount
    fn increase_assets<T>(&mut self, amount: f64, point: T)
    where T: Into<Option<NaiveDateTime>> {
        self.assets_ts.increment(amount, point);
    }

    /// Decrease assets by the given amount
    fn decrease_assets<T>(&mut self, amount: f64, point: T)
    where T: Into<Option<NaiveDateTime>> {
        self.assets_ts.decrement(amount, point);
    }

    /// Get the current amount of assets
    fn get_assets(&self) -> f64 {
        self.assets_ts.get_last_value()
    }
}


#[cfg(test)]
mod tests {
    #[test]
    fn test_increase_assets() {
        use super::*;

        let mut portfolio = Portfolio::new(100.0, 100.0, None);
        portfolio.increase_assets(10.0, None);
        assert_eq!(portfolio.get_assets(), 110.0);
    }

    #[test]
    fn test_decrease_assets() {
        use super::*;

        let mut portfolio = Portfolio::new(100.0, 100.0, None);
        portfolio.decrease_assets(10.0, None);
        assert_eq!(portfolio.get_assets(), 90.0);
    }
    
    #[test]
    fn test_get_assets() {
        use super::*;

        let mut portfolio = Portfolio::new(100.0, 100.0, None);
        assert_eq!(portfolio.get_assets(), 100.0);
        
        portfolio.increase_assets(10.0, None);
        assert_eq!(portfolio.get_assets(), 110.0);
        
        portfolio.decrease_assets(100.0, None);
        assert_eq!(portfolio.get_assets(), 10.0);
    }
}