use chrono::{NaiveDateTime, Utc};
use polars::prelude::*;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;

/// Create a DataFrame with a single row
///
/// This low-level helper function used when appending rows to a TrackedValue.
fn create_row<T>(value: Decimal, timestamp: T) -> DataFrame
where
    T: Into<Option<NaiveDateTime>>,
{
    let _timestamp = timestamp
        .into()
        .unwrap_or_else(|| NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap());
    df!(
        "timestamp" => [_timestamp],
        "value" => [value.to_f64().unwrap()]
    )
    .unwrap()
}

/// This struct is used to track a value as it changes over time.
///
/// It is specifically used to track the amount of assets and capital available to a portfolio
/// at any given point in time. The value is tracked as a total which is incremented and decremented.
///
/// It is a wrapper around a DataFrame with two columns: `timestamp` and `value`.
#[derive(Clone, Debug)]
pub struct TrackedValue(DataFrame);

impl Default for TrackedValue {
    fn default() -> Self {
        let ts_vec: Vec<NaiveDateTime> = vec![];
        let val_vec: Vec<f64> = vec![];
        TrackedValue(df!["timestamp" => ts_vec, "value" => val_vec].unwrap())
    }
}

impl TrackedValue {
    /// Create a new TrackedValue with an initial value and a starting point in time
    ///
    /// # Arguments
    ///
    /// * `amount` - The initial value of the tracked value
    /// * `timestamp` - The starting point in time
    pub fn with_initial<T>(amount: Decimal, timestamp: T) -> TrackedValue
    where
        T: Into<Option<NaiveDateTime>>,
    {
        let row = create_row(amount, timestamp);
        TrackedValue(row)
    }

    /// Add a new value to the tracked value
    ///
    /// This is a low-level interface and is not meant to be used directly.
    ///
    /// # Arguments
    /// * `amount` - The amount to add to the tracked value
    /// * `timestamp` - The point in time at which the value was added
    fn add_value<T>(&mut self, amount: Decimal, timestamp: T)
    where
        T: Into<Option<NaiveDateTime>>,
    {
        let row = create_row(amount, timestamp);
        self.0 = self.0.vstack(&row).unwrap();
    }

    /// Get the most recent value
    ///
    /// This is the main interface for retrieving the "value".
    pub fn get_last_value(&self) -> Decimal {
        // find last row
        let last_row = self
            .0
            .sort(
                ["timestamp"],
                SortMultipleOptions::default().with_nulls_last_multi([false, true]),
            )
            .unwrap()
            .tail(Some(1));

        // get value column
        let val = last_row.column("value").unwrap().get(0).unwrap();

        // extract value
        if let AnyValue::Float64(inner) = val {
            Decimal::from_f64(inner).unwrap()
        } else {
            panic!("Could not get last value from time-series chart")
        }
    }

    /// Decrement the tracked value by the given amount
    ///
    /// # Arguments
    /// * `amount` - The amount to decrement the total value by
    /// * `timestamp` - The point in time at which the total value was decremented
    pub fn decrement<T>(&mut self, amount: Decimal, timestamp: T)
    where
        T: Into<Option<NaiveDateTime>>,
    {
        let last_value = self.get_last_value();
        self.add_value(last_value - amount, timestamp);
    }

    /// Increment the tracked value by the given amount
    ///
    /// # Arguments
    /// * `amount` - The amount to increment the total value by
    /// * `timestamp` - The point in time at which the total value was incremented
    pub fn increment<T>(&mut self, amount: Decimal, timestamp: T)
    where
        T: Into<Option<NaiveDateTime>>,
    {
        let last_value = self.get_last_value();
        self.add_value(last_value + amount, timestamp);
    }
}

impl From<DataFrame> for TrackedValue {
    fn from(df: DataFrame) -> Self {
        TrackedValue(df)
    }
}

impl Into<DataFrame> for TrackedValue {
    fn into(self) -> DataFrame {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use rust_decimal_macros::dec;

    #[test]
    fn test_increment() {
        let start_time = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap();
        let start_val = dec!(1.0);
        let expected = dec!(2.0);

        let mut chart = TrackedValue::with_initial(start_val, start_time);
        for i in 0..10 {
            chart.increment(dec!(0.1), start_time + Duration::seconds(i));
        }

        let last_value = chart.get_last_value();
        assert_eq!(last_value, expected);
    }
    #[test]
    fn test_decrement() {
        let start_time = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap();
        let start_val = dec!(1.0);
        let expected = dec!(0.1);

        let mut chart = TrackedValue::with_initial(start_val, start_time);
        for i in 0..9 {
            chart.decrement(dec!(0.1), start_time + Duration::seconds(i));
        }

        let last_value = chart.get_last_value();
        assert_eq!(last_value, expected);
    }

    #[test]
    fn test_last_value() {
        let start_time = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap();
        let start_val = dec!(1.0);
        let expected = start_val + dec!(9.0);

        let mut chart = TrackedValue::with_initial(start_val, start_time);
        for i in 0..10 {
            chart.add_value(
                start_val + Decimal::from(i),
                start_time + Duration::seconds(i),
            );
        }

        let last_value = chart.get_last_value();
        assert_eq!(last_value, expected);
    }

    #[test]
    fn test_add_row() {
        // starting value and added value
        let start_val = dec!(1.0);
        let time = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap();

        let added_val = dec!(2.0);
        let added_time = time + Duration::seconds(1);

        // manually construct TimeSeriesChart
        let row = create_row(start_val, time);
        let mut chart = TrackedValue(row);

        // assert that initial value is correct
        assert_eq!(
            chart
                .0
                .column("value")
                .unwrap()
                .f64()
                .unwrap()
                .get(0)
                .unwrap(),
            start_val.to_f64().unwrap()
        );
        assert_eq!(
            chart
                .0
                .column("timestamp")
                .unwrap()
                .datetime()
                .unwrap()
                .get(0)
                .unwrap(),
            time.timestamp_millis()
        );

        chart.add_value(added_val, added_time);
        // assert that initial value remains after insertion and that timestamp is intact
        assert_eq!(
            chart
                .0
                .column("value")
                .unwrap()
                .f64()
                .unwrap()
                .get(0)
                .unwrap(),
            start_val.to_f64().unwrap()
        );
        assert_eq!(
            chart
                .0
                .column("timestamp")
                .unwrap()
                .datetime()
                .unwrap()
                .get(0)
                .unwrap(),
            time.timestamp_millis()
        );

        // assert that correct value is inserted and that the correct timestamp is used
        assert_eq!(
            chart
                .0
                .column("value")
                .unwrap()
                .f64()
                .unwrap()
                .get(1)
                .unwrap(),
            added_val.to_f64().unwrap()
        );
        assert_eq!(
            chart
                .0
                .column("timestamp")
                .unwrap()
                .datetime()
                .unwrap()
                .get(1)
                .unwrap(),
            added_time.timestamp_millis()
        );
    }

    #[test]
    fn test_from_dataframe() {
        // create a dataframe with 5 rows
        let df = df!(
            "timestamp" => [1, 2, 3, 4, 5],
            "value" => [1.0, 2.0, 3.0, 4.0, 5.0]
        )
        .unwrap();

        let tracked = TrackedValue::from(df);
        assert_eq!(tracked.0.shape(), (5, 2));

        for i in 1..6 {
            assert_eq!(
                tracked
                    .0
                    .column("timestamp")
                    .unwrap()
                    .i32()
                    .unwrap()
                    .get(i - 1)
                    .unwrap(),
                i as i32
            );
            assert_eq!(
                tracked
                    .0
                    .column("value")
                    .unwrap()
                    .f64()
                    .unwrap()
                    .get(i - 1)
                    .unwrap(),
                i as f64
            );
        }
    }

    #[test]
    fn test_into_dataframe() {
        // create a dataframe with 5 rows
        let expected_df = df!(
            "timestamp" => [1, 2, 3, 4, 5],
            "value" => [1.0, 2.0, 3.0, 4.0, 5.0]
        )
        .unwrap();

        let tracked = TrackedValue::from(expected_df.clone());

        let actual_df: DataFrame = tracked.into();

        assert_eq!(actual_df.shape(), (5, 2));
        assert_eq!(actual_df, expected_df);
    }
}
