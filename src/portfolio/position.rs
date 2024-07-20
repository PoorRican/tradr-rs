use crate::portfolio::Portfolio;
use crate::types::Side;
use crate::types::{ExecutedTrade, Trade};
use chrono::NaiveDateTime;
use polars::frame::DataFrame;
use polars::prelude::*;
use rust_decimal::Decimal;

pub trait PositionHandlers {
    fn add_open_position(&mut self, trade: &ExecutedTrade);

    fn get_open_positions(&self) -> Option<Vec<&ExecutedTrade>>;

    fn select_open_positions_by_price(&self, price: Decimal) -> Option<Vec<&ExecutedTrade>>;
    fn available_open_positions(&self) -> usize;
    fn clear_open_positions(&mut self, executed_trade: &ExecutedTrade);
}

impl PositionHandlers for Portfolio {
    /// Add provided trade as an open position
    ///
    /// This is intended to be called after a buy trade has been executed. The timestamp of the
    /// executed trade is added to the `open_positions` vector. The timestamp is used to track
    /// and the timestamp is removed from the `open_positions` vector by the `clear_open_positions`
    /// method which uses `select_open_positions` to select open positions that were closed.
    ///
    /// # Arguments
    /// * `trade` - The executed trade to add. Only buy trades are added. Sell trades are ignored.
    fn add_open_position(&mut self, trade: &ExecutedTrade) {
        if trade.get_side() == Side::Buy {
            self.open_positions.push(*trade.get_timestamp());
        }
    }

    /// Returns a [`Vec`] with references to the executed trades that correspond to open positions.
    ///
    /// If there are no open positions, `None` is returned.
    fn get_open_positions(&self) -> Option<Vec<&ExecutedTrade>> {
        if self.open_positions.is_empty() {
            return None;
        }

        Some(self.open_positions
            .iter().map(
                |x| self.executed_trades.get(x).unwrap()
            ).collect::<Vec<_>>())
    }

    /// Select open positions that are less than the given `price` which may be profitable.
    ///
    /// `None` is returned if there are no open positions or no open positions that are less than price.
    fn select_open_positions_by_price(&self, price: Decimal) -> Option<Vec<&ExecutedTrade>> {
        if self.open_positions.is_empty() {
            return None;
        }

        let selected = self.open_positions.iter().map(
                |x| self.executed_trades.get(x).unwrap()
            ).filter(|x| x.get_price() <= price).collect::<Vec<_>>();

        if selected.is_empty() {
            None
        } else {
            Some(selected)
        }
    }

    /// Return the number of available open positions
    ///
    /// This is used for limiting risk buy preventing too many open positions.
    /// The intention is to prevent any buy trades from being executed if there are too many open positions.
    /// Therefore, when this value is 0, no buy trades should be attempted.
    fn available_open_positions(&self) -> usize {
        self.open_positions_limit - self.open_positions.len()
    }

    /// Clear the open positions that were closed by the executed trade
    ///
    /// This is intended to be called after a sell trade has been executed.
    ///
    /// Clearing of open positions is totally dependent on the price/rate of the
    /// executed trade. The amount of trade is not taken into consideration because it
    /// is assumed that the entire open position was closed. Additionally, the executed trade
    /// passed is not to have required to have closed any open positions. The method
    /// `select_open_positions` is relied upon to select open positions both by this method
    /// and before the executed trade is attempted.
    ///
    /// # Arguments
    /// * `executed_trade` - The executed trade that may have closed any open positions
    fn clear_open_positions(&mut self, executed_trade: &ExecutedTrade) {
        if executed_trade.get_side() != Side::Sell {
            return;
        }

        // TODO: there needs to be a better way to handle how positions are closed
        let open_positions = self.select_open_positions_by_price(executed_trade.get_price());

        if let Some(open_positions) = open_positions {
            // get the timestamps of the open positions
            let open_positions_points = open_positions
                .iter()
                .map(|x| x.get_timestamp())
                .collect::<Vec<_>>();

            // remove the timestamps of the open positions that were closed by the executed trade
            self.open_positions = self
                .open_positions
                .iter()
                .filter(|x| !open_positions_points.contains(x))
                .map(|x| *x)
                .collect();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::portfolio::{Portfolio, PositionHandlers, TradeHandlers};
    use crate::types::{ExecutedTrade, Side, Trade};
    use chrono::{Duration, NaiveDateTime, Utc};
    use rust_decimal::Decimal;
    use rust_decimal::prelude::FromPrimitive;
    use rust_decimal_macros::dec;

    /// Test that open positions are correctly added to the `open_positions` vector.
    /// Also ensure that closed positions are not added to the `open_positions` vector.
    #[test]
    fn test_set_as_open_position() {
        let time = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap();

        let mut portfolio = Portfolio::new(dec!(100.0), dec!(100.0), None);

        assert!(portfolio.open_positions.is_empty());

        // add a buy and assert it is added to `open_positions`
        let trade = ExecutedTrade::with_calculated_notional("id".to_string(), Side::Buy, dec!(1.0), dec!(1.0), time);
        portfolio.add_open_position(&trade);
        assert_eq!(portfolio.open_positions.len(), 1);

        // add a sell and assert it is *not* added to `open_positions`
        let trade = ExecutedTrade::with_calculated_notional(
            "id".to_string(),
            Side::Sell,
            dec!(1.0),
            dec!(1.0),
            time + Duration::minutes(1),
        );
        portfolio.add_open_position(&trade);
        assert_eq!(portfolio.open_positions.len(), 1);

        // add another buy and assert it is added to `open_positions`
        let time2 = time + Duration::minutes(2);
        let trade = ExecutedTrade::with_calculated_notional("id".to_string(), Side::Buy, dec!(1.0), dec!(1.0), time2);
        portfolio.add_open_position(&trade);
        assert_eq!(portfolio.open_positions.len(), 2);

        // ensure that the time values are correct in `open_positions`
        assert!(portfolio.open_positions.contains(&time));
        assert!(portfolio.open_positions.contains(&time2));
    }

    #[test]
    fn test_get_open_positions() {
        let mut portfolio = Portfolio::new(dec!(100.0), dec!(100.0), None);
        assert_eq!(portfolio.get_open_positions(), None);

        // create some executed trades
        // only `trade` and `trade3` should be returned by `get_open_positions`
        let time = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap();
        let trade = ExecutedTrade::with_calculated_notional("id".to_string(), Side::Buy, dec!(1.0), dec!(1.0), time);
        let trade2 = ExecutedTrade::with_calculated_notional(
            "id".to_string(),
            Side::Buy,
            dec!(1.5),
            dec!(0.9),
            time + Duration::seconds(1),
        );
        let trade3 = ExecutedTrade::with_calculated_notional(
            "id".to_string(),
            Side::Buy,
            dec!(1.7),
            dec!(1.5),
            time + Duration::seconds(2),
        );

        portfolio.add_executed_trade(trade);
        portfolio.add_executed_trade(trade2);
        portfolio.add_executed_trade(trade3);

        // assert that the dataframe returned by `get_open_positions` corresponds to open trades
        assert_eq!(portfolio.get_open_positions().unwrap().len(), 3);

        let expected_price_sum = Decimal::from_f64(1.0 + 1.5 + 1.7).unwrap();
        let expected_quantity_sum = Decimal::from_f64(1.0 + 0.9 + 1.5).unwrap();

        let open_positions = portfolio.get_open_positions().unwrap();
        assert_eq!(
            open_positions
                .iter().map(|x| x.get_quantity())
                .sum::<Decimal>(),
            expected_quantity_sum
        );
        assert_eq!(
            open_positions
                .iter().map(|x| x.get_price())
                .sum::<Decimal>(),
            expected_price_sum
        );
    }

    #[test]
    fn test_select_open_positions() {
        let time = Utc::now().naive_utc();
        let trade = ExecutedTrade::with_calculated_notional(
            "id".to_string(),
            Side::Buy,
            dec!(2.0),
            dec!(1.0),
            time + Duration::seconds(1),
        );
        let trade2 = ExecutedTrade::with_calculated_notional(
            "id".to_string(),
            Side::Buy,
            dec!(1.9),
            dec!(1.0),
            time + Duration::seconds(2),
        );
        let trade3 = ExecutedTrade::with_calculated_notional(
            "id".to_string(),
            Side::Buy,
            dec!(1.8),
            dec!(1.0),
            time + Duration::seconds(3),
        );
        let trade4 = ExecutedTrade::with_calculated_notional(
            "id".to_string(),
            Side::Buy,
            dec!(1.0),
            dec!(1.0),
            time + Duration::seconds(4),
        );
        let trade5 = ExecutedTrade::with_calculated_notional(
            "id".to_string(),
            Side::Buy,
            dec!(0.1),
            dec!(1.0),
            time + Duration::seconds(5),
        );

        let mut portfolio = Portfolio::new(dec!(100.0), dec!(100.0), None);

        // assert that `None` is returned when there are no open positions
        assert_eq!(portfolio.select_open_positions_by_price(dec!(1.0)), None);

        // add trades to `executed_trades`
        portfolio.add_executed_trade(trade);
        portfolio.add_executed_trade(trade2);
        portfolio.add_executed_trade(trade3);
        portfolio.add_executed_trade(trade4);
        portfolio.add_executed_trade(trade5);

        // remove last trade from `open_positions`
        portfolio.open_positions.pop();

        // assert that `None` is returned when price is 0.9
        let selected_open_positions = portfolio.select_open_positions_by_price(dec!(0.9));
        assert_eq!(selected_open_positions, None);

        // assert that the correct number of open positions are returned
        // we will be selecting trades that are less than 1.9
        // therefore only `trade2`, `trade3`, and `trade4` should be returned
        let selected_open_positions = portfolio.select_open_positions_by_price(dec!(1.9)).unwrap();

        assert_eq!(selected_open_positions.len(), 3);
        assert_eq!(
            selected_open_positions
                .iter().map(|x| x.get_price())
                .sum::<Decimal>(),
            Decimal::from_f64(1.9 + 1.8 + 1.0).unwrap()
        );
        assert_eq!(
            selected_open_positions
                .iter().map(|x| x.get_quantity())
                .sum::<Decimal>(),
            Decimal::from_f64(1.0 * 3.0).unwrap()
        );
    }

    #[test]
    fn test_available_open_positions() {
        let mut portfolio = Portfolio::new(dec!(100.0), dec!(100.0), None);

        // assert that `available_open_positions` is maxed when there are no open positions
        portfolio.open_positions_limit = 10;
        assert_eq!(portfolio.available_open_positions(), 10);

        // assert that `available_open_positions` is correctly decremented when an open positions are added
        portfolio
            .open_positions
            .push(NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap());
        assert_eq!(portfolio.available_open_positions(), 9);

        portfolio
            .open_positions
            .push(NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap());
        assert_eq!(portfolio.available_open_positions(), 8);

        // assert that `available_open_positions` is 0 when `open_positions_limit` is reached
        portfolio.open_positions_limit = 2;
        assert_eq!(portfolio.available_open_positions(), 0);
    }

    #[test]
    fn test_clear_open_positions() {
        let mut portfolio = Portfolio::new(dec!(100.0), dec!(100.0), None);

        // create some open positions with varying prices
        let time = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap();
        let trade = ExecutedTrade::with_calculated_notional(
            "id".to_string(),
            Side::Buy,
            dec!(2.0),
            dec!(1.0),
            time + Duration::seconds(1),
        );
        let trade2 = ExecutedTrade::with_calculated_notional(
            "id".to_string(),
            Side::Buy,
            dec!(1.9),
            dec!(1.0),
            time + Duration::seconds(2),
        );
        let trade3 = ExecutedTrade::with_calculated_notional(
            "id".to_string(),
            Side::Buy,
            dec!(1.8),
            dec!(1.0),
            time + Duration::seconds(3),
        );
        let trade4 = ExecutedTrade::with_calculated_notional(
            "id".to_string(),
            Side::Buy,
            dec!(1.0),
            dec!(1.0),
            time + Duration::seconds(4),
        );

        // add trades to `executed_trades`
        portfolio.add_executed_trade(trade);
        portfolio.add_executed_trade(trade2);
        portfolio.add_executed_trade(trade3);
        portfolio.add_executed_trade(trade4);

        assert_eq!(portfolio.open_positions.len(), 4);

        // remove the lowest buy
        let executed_trade = ExecutedTrade::with_calculated_notional(
            "id".to_string(),
            Side::Sell,
            dec!(1.1),
            dec!(1.0),
            time + Duration::seconds(5),
        );
        portfolio.clear_open_positions(&executed_trade);
        assert_eq!(portfolio.open_positions.len(), 3);

        // assert that 2/3 of the remaining positions are cleared when price is 1.9
        let executed_trade = ExecutedTrade::with_calculated_notional(
            "id".to_string(),
            Side::Sell,
            dec!(1.9),
            dec!(1.0),
            time + Duration::seconds(6),
        );
        portfolio.clear_open_positions(&executed_trade);
        assert_eq!(portfolio.open_positions.len(), 1);
        assert_eq!(portfolio.open_positions[0], time + Duration::seconds(1));

        // assert that all positions are cleared when price is 2.0
        let executed_trade = ExecutedTrade::with_calculated_notional(
            "id".to_string(),
            Side::Sell,
            dec!(2.0),
            dec!(1.0),
            time + Duration::seconds(6),
        );
        portfolio.clear_open_positions(&executed_trade);
        assert!(portfolio.open_positions.is_empty());
    }
}
