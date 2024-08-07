use crate::portfolio::{OpenPosition, Portfolio};
use crate::types::Side;
use crate::types::{ExecutedTrade, Trade};
use chrono::NaiveDateTime;
use rust_decimal::Decimal;
use std::collections::BTreeMap;

/// Tracking and management of open positions
pub trait PositionHandlers {
    fn add_open_position(&mut self, trade: &ExecutedTrade);

    fn get_open_positions_as_trades(&self) -> Option<Vec<&ExecutedTrade>>;
    fn get_open_positions(&self) -> &BTreeMap<NaiveDateTime, OpenPosition>;
    fn close_positions(&mut self, quantity: Decimal, close_price: Decimal) -> Vec<String>;
    fn update_position_metrics(&mut self);
    fn total_open_quantity(&self) -> Decimal;
    fn average_entry_price(&self) -> Decimal;
    fn total_position_value(&self) -> Decimal;
}

impl PositionHandlers for Portfolio {
    /// Add provided trade as an open position
    ///
    /// This is intended to be called after a buy trade has been executed. The timestamp of the
    /// executed trade is added to the `open_positions` map. The timestamp is used to track
    ///
    /// # Panics
    ///
    /// Will not accept sell trades
    fn add_open_position(&mut self, trade: &ExecutedTrade) {
        if trade.get_side() == Side::Sell {
            // TODO: return an err instead
            panic!("Attempted to add a sell trade as an open position");
        }

        let position = OpenPosition {
            entry_price: trade.get_price(),
            quantity: trade.get_quantity(),
            entry_time: *trade.get_timestamp(),
            order_id: trade.get_order_id().to_string(),
        };

        self.open_positions.insert(*trade.get_timestamp(), position);
        self.update_position_metrics();
    }

    /// Returns a [`Vec`] with references to the executed trades that correspond to open positions.
    ///
    /// If there are no open positions, `None` is returned.
    fn get_open_positions_as_trades(&self) -> Option<Vec<&ExecutedTrade>> {
        if self.open_positions.is_empty() {
            return None;
        }

        Some(
            self.open_positions
                .keys()
                .map(|x| self.executed_trades.get(x).unwrap())
                .collect::<Vec<_>>(),
        )
    }

    fn get_open_positions(&self) -> &BTreeMap<NaiveDateTime, OpenPosition> {
        &self.open_positions
    }

    /// Close open positions by quantity and close price
    ///
    /// First, profitable positions are closed first, from most-profitable to least. Then, non-profitable positions are
    /// closed in a FIFO order.
    ///
    /// Returns the order ids of the fully closed positions.
    fn close_positions(&mut self, quantity: Decimal, close_price: Decimal) -> Vec<String> {
        let mut remaining_quantity = quantity;
        let mut closed_trade_ids = Vec::new();
        let mut positions_to_remove = Vec::new();
        let mut positions_to_update = Vec::new();

        // Sort positions by profitability (most profitable first)
        let mut sorted_positions: Vec<_> = self.open_positions.iter().collect();
        sorted_positions.sort_by(|a, b| {
            let profit_a = close_price - a.1.entry_price;
            let profit_b = close_price - b.1.entry_price;
            profit_b
                .partial_cmp(&profit_a)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        for (timestamp, position) in sorted_positions {
            if remaining_quantity <= Decimal::ZERO {
                break;
            }

            if position.quantity <= remaining_quantity {
                remaining_quantity -= position.quantity;
                closed_trade_ids.push(position.order_id.clone());
                positions_to_remove.push(*timestamp);
            } else {
                let new_quantity = position.quantity - remaining_quantity;
                positions_to_update.push((*timestamp, new_quantity));
                remaining_quantity = Decimal::ZERO;
            }
        }

        // Remove fully closed positions
        for timestamp in positions_to_remove {
            self.open_positions.remove(&timestamp);
        }

        // Update partially closed positions
        for (timestamp, new_quantity) in positions_to_update {
            if let Some(position) = self.open_positions.get_mut(&timestamp) {
                position.quantity = new_quantity;
            }
        }

        self.update_position_metrics();
        closed_trade_ids
    }

    /// Update the average entry price and total notional value of open positions
    fn update_position_metrics(&mut self) {
        let (total_value, total_cost, total_quantity) = self.open_positions.values().fold(
            (Decimal::ZERO, Decimal::ZERO, Decimal::ZERO),
            |acc, position| {
                (
                    acc.0 + position.quantity * position.entry_price,
                    acc.1 + position.quantity * position.entry_price,
                    acc.2 + position.quantity,
                )
            },
        );

        self.total_position_notional_value = total_value;
        self.average_entry_price = if total_quantity.is_zero() {
            Decimal::ZERO
        } else {
            total_cost / total_quantity
        };
    }

    /// Total quantity of open positions
    fn total_open_quantity(&self) -> Decimal {
        self.open_positions.values().map(|p| p.quantity).sum()
    }

    /// Average entry price of open positions
    fn average_entry_price(&self) -> Decimal {
        self.average_entry_price
    }

    /// Total notional value of open positions
    fn total_position_value(&self) -> Decimal {
        self.total_position_notional_value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, NaiveTime};
    use rust_decimal_macros::dec;

    fn create_executed_trade(
        id: &str,
        side: Side,
        price: Decimal,
        quantity: Decimal,
        timestamp: NaiveDateTime,
    ) -> ExecutedTrade {
        ExecutedTrade::with_calculated_notional(id.to_string(), side, price, quantity, timestamp)
    }

    #[test]
    fn test_add_open_position() {
        let mut portfolio = Portfolio::default();
        let timestamp = NaiveDate::from_ymd_opt(2023, 1, 1)
            .unwrap()
            .and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap());
        let trade = create_executed_trade("1", Side::Buy, dec!(100), dec!(10), timestamp);

        portfolio.add_open_position(&trade);

        assert_eq!(portfolio.open_positions.len(), 1);
        assert_eq!(portfolio.total_position_notional_value, dec!(1000)); // 100 * 10
        assert_eq!(portfolio.average_entry_price, dec!(100));

        let position = portfolio.open_positions.get(&timestamp).unwrap();
        assert_eq!(position.entry_price, dec!(100));
        assert_eq!(position.quantity, dec!(10));
        assert_eq!(position.entry_time, timestamp);
        assert_eq!(position.order_id, "1");
    }

    #[test]
    fn test_update_position_metrics() {
        let mut portfolio = Portfolio::default();
        let timestamp1 = NaiveDate::from_ymd_opt(2023, 1, 1)
            .unwrap()
            .and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap());
        let timestamp2 = NaiveDate::from_ymd_opt(2023, 1, 2)
            .unwrap()
            .and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap());

        portfolio.open_positions.insert(
            timestamp1,
            OpenPosition {
                entry_price: dec!(100),
                quantity: dec!(10),
                entry_time: timestamp1,
                order_id: "1".to_string(),
            },
        );
        portfolio.open_positions.insert(
            timestamp2,
            OpenPosition {
                entry_price: dec!(110),
                quantity: dec!(5),
                entry_time: timestamp2,
                order_id: "2".to_string(),
            },
        );

        portfolio.update_position_metrics();

        assert_eq!(portfolio.total_position_notional_value, dec!(1550)); // (100 * 10) + (110 * 5)
        assert!(
            portfolio.average_entry_price > dec!(103.3333)
                && portfolio.average_entry_price < dec!(103.3334)
        ); // (1000 + 550) / 15
    }

    #[test]
    fn test_close_positions() {
        let mut portfolio = Portfolio::default();
        let timestamp1 = NaiveDate::from_ymd_opt(2023, 1, 1)
            .unwrap()
            .and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap());
        let timestamp2 = NaiveDate::from_ymd_opt(2023, 1, 2)
            .unwrap()
            .and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap());
        let timestamp3 = NaiveDate::from_ymd_opt(2023, 1, 3)
            .unwrap()
            .and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap());

        portfolio.open_positions.insert(
            timestamp1,
            OpenPosition {
                entry_price: dec!(100),
                quantity: dec!(10),
                entry_time: timestamp1,
                order_id: "1".to_string(),
            },
        );
        portfolio.open_positions.insert(
            timestamp2,
            OpenPosition {
                entry_price: dec!(110),
                quantity: dec!(5),
                entry_time: timestamp2,
                order_id: "2".to_string(),
            },
        );
        portfolio.open_positions.insert(
            timestamp3,
            OpenPosition {
                entry_price: dec!(90),
                quantity: dec!(8),
                entry_time: timestamp3,
                order_id: "3".to_string(),
            },
        );

        portfolio.update_position_metrics();

        // Close some positions
        let closed_trade_ids = portfolio.close_positions(dec!(18), dec!(120));

        // Check that the most profitable positions were closed first
        assert_eq!(closed_trade_ids, vec!["3".to_string(), "1".to_string()]);
        assert_eq!(portfolio.open_positions.len(), 1);

        let remaining_position = portfolio.open_positions.get(&timestamp2).unwrap();
        assert_eq!(remaining_position.quantity, dec!(5)); // 8 - (18 - 15) = 5

        assert_eq!(portfolio.total_position_notional_value, dec!(550)); // 110 * 5
        assert_eq!(portfolio.average_entry_price, dec!(110));
    }

    #[test]
    fn test_close_positions_partial() {
        let mut portfolio = Portfolio::default();
        let timestamp = NaiveDate::from_ymd_opt(2023, 1, 1)
            .unwrap()
            .and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap());

        portfolio.open_positions.insert(
            timestamp,
            OpenPosition {
                entry_price: dec!(100),
                quantity: dec!(10),
                entry_time: timestamp,
                order_id: "1".to_string(),
            },
        );

        portfolio.update_position_metrics();

        // Partially close the position
        let closed_trade_ids = portfolio.close_positions(dec!(6), dec!(120));

        assert!(closed_trade_ids.is_empty()); // No trades fully closed
        assert_eq!(portfolio.open_positions.len(), 1);

        let remaining_position = portfolio.open_positions.get(&timestamp).unwrap();
        assert_eq!(remaining_position.quantity, dec!(4));

        assert_eq!(portfolio.total_position_notional_value, dec!(400)); // 100 * 4
        assert_eq!(portfolio.average_entry_price, dec!(100));
    }

    #[test]
    fn test_close_positions_multiple_partial() {
        let mut portfolio = Portfolio::default();
        let timestamp1 = NaiveDate::from_ymd_opt(2023, 1, 1)
            .unwrap()
            .and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap());
        let timestamp2 = NaiveDate::from_ymd_opt(2023, 1, 2)
            .unwrap()
            .and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap());

        portfolio.open_positions.insert(
            timestamp1,
            OpenPosition {
                entry_price: dec!(100),
                quantity: dec!(10),
                entry_time: timestamp1,
                order_id: "1".to_string(),
            },
        );
        portfolio.open_positions.insert(
            timestamp2,
            OpenPosition {
                entry_price: dec!(110),
                quantity: dec!(5),
                entry_time: timestamp2,
                order_id: "2".to_string(),
            },
        );

        portfolio.update_position_metrics();

        // Close more than one position, but not all
        let closed_trade_ids = portfolio.close_positions(dec!(12), dec!(120));

        assert_eq!(closed_trade_ids, vec!["1".to_string()]); // Only the first trade is fully closed
        assert_eq!(portfolio.open_positions.len(), 1);

        let remaining_position = portfolio.open_positions.get(&timestamp2).unwrap();
        assert_eq!(remaining_position.quantity, dec!(3)); // 10 - (12 - 5) = 3

        assert_eq!(portfolio.total_position_notional_value, dec!(330)); // 110 * 3
        assert_eq!(portfolio.average_entry_price, dec!(110));
    }
}
