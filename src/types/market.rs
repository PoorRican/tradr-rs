use crate::traits::AsDataFrame;
use crate::utils;
use polars::prelude::DataFrame;
use sqlite::Connection;
use std::collections::HashMap;

/// Path to the database file
const DB_PATH: &str = "data/candle_data.sqlite3";

/// Intraday frequency names ordered by priority
const INTRADAY_FREQUENCIES: [&str; 6] = ["1m", "5m", "15m", "1h", "6h", "1d"];

#[derive(Debug)]
pub enum MarketDataError {
    FrequencyNotFound,
}

#[derive(Debug)]
pub struct MarketData {
    /// Used to identify the asset
    pub asset_name: String,
    pub candles: HashMap<String, DataFrame>,
}

impl MarketData {
    /// Create a new [`MarketData`] instance from the database
    pub fn from_db<S: Into<String>>(asset_name: S) -> Self {
        let asset_name = asset_name.into();
        let table_names = get_relevant_table_names(&asset_name);

        let candles = table_names
            .into_iter()
            .map(|table_name| {
                let df = utils::extract_candles_from_db(DB_PATH, &table_name)
                    .unwrap()
                    .as_dataframe();

                let frequency = extract_frequency_from_table_name(&table_name);
                (frequency, df)
            })
            .collect();

        MarketData {
            asset_name,
            candles,
        }
    }

    pub fn get_candles(&self, frequency: &String) -> Result<&DataFrame, MarketDataError> {
        if let Some(candles) = self.candles.get(frequency) {
            Ok(candles)
        } else {
            Err(MarketDataError::FrequencyNotFound)
        }
    }
}

/// Retrieves all table names that contain the given substring.
///
/// Used to find all tables relevant to a given asset name
fn get_relevant_table_names(substring: &String) -> Vec<String> {
    let conn = Connection::open(DB_PATH).unwrap();
    conn.prepare("SELECT name FROM sqlite_master WHERE type='table'")
        .unwrap()
        .into_iter()
        .map(|row| {
            let data = row.unwrap();
            data.read::<&str, _>(0).to_owned()
        })
        .filter(|table| {
            let lowercase_substring = substring.to_lowercase();
            table.to_lowercase().contains(lowercase_substring.as_str())
        })
        .map(|table| table.to_string())
        .collect()
}

/// Extracts the frequency from the table name
fn extract_frequency_from_table_name(table_name: &String) -> String {
    INTRADAY_FREQUENCIES
        .iter()
        .find(|&freq| table_name.contains(freq))
        .expect("Could not extract frequency from table name")
        .to_string()
}
