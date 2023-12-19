use chrono::{NaiveDateTime, Utc};
use polars::prelude::*;

/// Create a DataFrame with a single row
///
/// This low-level helper function used when appending rows to a TrackedValue.
fn create_row<T>(value: f64, timestamp: T) -> DataFrame
where T: Into<Option<NaiveDateTime>> {
    let _timestamp = timestamp.into().unwrap_or_else(|| NaiveDateTime::from_timestamp_opt(
        Utc::now().timestamp(),
        0)
        .unwrap());
    df!(
        "timestamp" => [_timestamp],
        "value" => [value]
    ).unwrap()
}

/// This struct is used to track a value as it changes over time.
///
/// It is specifically used to track the amount of assets and capital available to a portfolio
/// at any given point in time. The value is tracked as a total which is incremented and decremented.
/// 
/// It is a wrapper around a DataFrame with two columns: `timestamp` and `value`.
#[derive(Clone)]
pub struct TrackedValue(DataFrame);

impl TrackedValue {
    /// Create a new TrackedValue with an initial value and a starting point in time
    /// 
    /// # Arguments
    /// 
    /// * `amount` - The initial value of the tracked value
    /// * `timestamp` - The starting point in time
    pub fn with_initial<T>(amount: f64, timestamp: T) -> TrackedValue
    where T: Into<Option<NaiveDateTime>> {
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
    fn add_value<T>(&mut self, amount: f64, timestamp: T)
    where T: Into<Option<NaiveDateTime>> {
        let row = create_row(amount, timestamp);
        self.0 = self.0.vstack(&row).unwrap();
    }

    /// Get the most recent value
    /// 
    /// This is the main interface for retrieving the "value".
    pub fn get_last_value(&self) -> f64 {
        // find last row
        let last_row = self.0
            .sort(["timestamp"], false, true)
            .unwrap()
            .tail(Some(1));

        // get value column
        let val = last_row
            .column("value")
            .unwrap()
            .get(0)
            .unwrap();

        // extract value
        if let AnyValue::Float64(inner) = val {
            inner
        } else {
            panic!("Could not get last value from time-series chart")
        }
    }

    /// Decrement the tracked value by the given amount
    /// 
    /// # Arguments
    /// * `amount` - The amount to decrement the total value by
    /// * `timestamp` - The point in time at which the total value was decremented
    pub fn decrement<T>(&mut self, amount: f64, timestamp: T)
    where T: Into<Option<NaiveDateTime>> {
        let last_value = self.get_last_value();
        self.add_value(last_value - amount, timestamp);
    }

    /// Increment the tracked value by the given amount
    /// 
    /// # Arguments
    /// * `amount` - The amount to increment the total value by
    /// * `timestamp` - The point in time at which the total value was incremented
    pub fn increment<T>(&mut self, amount: f64, timestamp: T)
    where T: Into<Option<NaiveDateTime>> {
        let last_value = self.get_last_value();
        self.add_value(last_value + amount, timestamp);
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    /// truncate float to 2 decimal places
    fn trunc_float(f: f64) -> f64 {
        (f * 100.0).trunc() / 100.0
    }

    #[test]
    fn test_increment() {
        let start_time = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap();
        let start_val = 1.0;
        let expected = 2.0;

        let mut chart = TrackedValue::with_initial(start_val, start_time);
        for i in 0..10 {
            chart.increment(0.1, start_time + Duration::seconds(i));
        }

        let last_value = chart.get_last_value();
        assert_eq!(trunc_float(last_value), expected);
    }
    #[test]
    fn test_decrement() {
        let start_time = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap();
        let start_val = 1.0;
        let expected = 0.1;

        let mut chart = TrackedValue::with_initial(start_val, start_time);
        for i in 0..9 {
            chart.decrement(0.1, start_time + Duration::seconds(i));
        }

        let last_value = chart.get_last_value();
        assert_eq!(trunc_float(last_value), expected);
    }

    #[test]
    fn test_last_value() {
        let start_time = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap();
        let start_val = 1.0;
        let expected = start_val + 9.0;

        let mut chart = TrackedValue::with_initial(start_val, start_time);
        for i in 0..10 {
            chart.add_value(start_val + i as f64, start_time + Duration::seconds(i));
        }

        let last_value = chart.get_last_value();
        assert_eq!(last_value, expected);
    }

    #[test]
    fn test_add_row() {
        // starting value and added value
        let start_val = 1.0;
        let time = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap();

        let added_val = 2.0;
        let added_time = time + Duration::seconds(1);

        // manually construct TimeSeriesChart
        let row = create_row(start_val, time);
        let mut chart = TrackedValue(row);

        // assert that initial value is correct
        assert_eq!(chart.0.column("value").unwrap().f64().unwrap().get(0).unwrap(), start_val);
        assert_eq!(chart.0.column("timestamp").unwrap().datetime().unwrap().get(0).unwrap(), time.timestamp_millis());

        chart.add_value(added_val, added_time);
        // assert that initial value remains after insertion and that timestamp is intact
        assert_eq!(chart.0.column("value").unwrap().f64().unwrap().get(0).unwrap(), start_val);
        assert_eq!(chart.0.column("timestamp").unwrap().datetime().unwrap().get(0).unwrap(), time.timestamp_millis());

        // assert that correct value is inserted and that the correct timestamp is used
        assert_eq!(chart.0.column("value").unwrap().f64().unwrap().get(1).unwrap(), added_val);
        assert_eq!(chart.0.column("timestamp").unwrap().datetime().unwrap().get(1).unwrap(), added_time.timestamp_millis());
    }
}