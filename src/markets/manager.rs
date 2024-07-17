use crate::markets::BaseMarket;
use crate::traits::AsDataFrame;
use crate::utils::extract_new_rows;
use polars::error::PolarsResult;
use polars::frame::{DataFrame, UniqueKeepStrategy};
use polars::prelude::*;
use polars_io::{SerReader, SerWriter};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Error;
use std::path::Path;

pub const VALID_INTERVALS: [&str; 6] = ["1m", "5m", "15m", "1h", "6h", "1d"];

/// Updates the existing data frame by appending the new data frame.
///
/// Any rows that have the same time value will be overwritten.
///
/// The array is sorted by time in descending order.
///
/// # Arguments
/// * `existing` - Reference to the existing data frame.
/// * `new_candles` - The new data frame to update the existing data frame with.
///
/// # Returns
/// * `DataFrame` - The updated data frame with new candles
fn append_candles(existing: &DataFrame, new_candles: DataFrame) -> PolarsResult<DataFrame> {
    let appended = existing.vstack(&new_candles)?;

    let mut unique =
        appended.unique_stable(Some(&["time".to_string()]), UniqueKeepStrategy::Last, None)?;

    unique.sort(
        ["time"],
        SortMultipleOptions::new().with_order_descending_multi([true, false]),
    )
}

fn save_candles(file_path: &Path, data: &mut DataFrame) -> Result<(), Error> {
    if file_path.is_dir() {
        return Err(Error::new(
            std::io::ErrorKind::InvalidInput,
            "A directory was passed. Path must be a file",
        ));
    }

    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(file_path)?;

    CsvWriter::new(file)
        .include_header(true)
        .finish(data)
        .unwrap();

    Ok(())
}

fn load_candles(file_path: &Path) -> Result<DataFrame, Error> {
    if !file_path.is_file() {
        return Err(Error::new(
            std::io::ErrorKind::InvalidInput,
            "path must be a file",
        ));
    }

    let df = CsvReadOptions::default()
        .try_into_reader_with_file_path(Some(file_path.into()))
        .unwrap()
        .finish()
        .unwrap();
    Ok(df)
}

pub struct CandleManager<'a, T>
where
    T: BaseMarket,
{
    candles: HashMap<String, DataFrame>,
    pair: String,
    market: &'a T,
}

impl<'a, T> CandleManager<'a, T>
where
    T: BaseMarket,
{
    pub fn new(pair: &str, market: &'a T) -> Self {
        // TODO: implement a default path for storing all candle data
        Self {
            candles: HashMap::new(),
            pair: pair.to_string(),
            market,
        }
    }

    pub fn get(&self, interval: &str) -> Option<&DataFrame> {
        self.candles.get(&interval.to_string())
    }

    pub async fn update(&mut self, interval: &str) -> Option<DataFrame> {
        let candles = self.market.get_candles(&self.pair, interval).await.unwrap();
        let df = candles.as_dataframe();
        match self.candles.get(interval) {
            Some(existing) => {
                let updated = append_candles(existing, df).unwrap();
                let new_row = extract_new_rows(&updated, existing);
                self.candles.insert(interval.to_string(), updated);
                Some(new_row)
            }
            None => {
                self.candles.insert(interval.to_string(), df);
                None
            }
        }
    }

    pub async fn update_all(&mut self) {
        for interval in VALID_INTERVALS.iter() {
            self.update(interval).await;
        }
    }

    pub fn save(&mut self, path: &Path) -> Result<(), Error> {
        for (interval, df) in self.candles.iter_mut() {
            let file_path = path.join(format!("{}.csv", interval));
            save_candles(&file_path, df)?;
        }
        Ok(())
    }

    pub fn load(&mut self, path: &Path) -> Result<(), Error> {
        for interval in VALID_INTERVALS.iter() {
            let file_path = path.join(format!("{}.csv", interval));
            let df = load_candles(&file_path)?;
            self.candles.insert(interval.to_string(), df);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::markets::manager::{load_candles, CandleManager, VALID_INTERVALS};
    use crate::markets::CoinbaseClient;
    use crate::utils::create_temp_dir;
    use polars::frame::DataFrame;
    use polars::prelude::*;
    use std::fs::remove_dir_all;
    use std::path::Path;

    const TEST_DIR: &str = "candle_manager_testing";

    fn create_df() -> DataFrame {
        df!(
            "time" => &[6, 5, 4, 3],
            "open" => &[1.0, 2.0, 3.0, 4.0],
            "high" => &[1.0, 2.0, 3.0, 4.0],
            "low" => &[1.0, 2.0, 3.0, 4.0],
            "close" => &[1.0, 2.0, 3.0, 4.0],
            "volume" => &[1.0, 2.0, 3.0, 4.0]
        )
        .unwrap()
    }

    fn build_market() -> CoinbaseClient {
        CoinbaseClient::new()
    }

    fn create_manager(market: &CoinbaseClient) -> CandleManager<CoinbaseClient> {
        let mut manager = CandleManager::new("BTC-USD", market);

        for interval in VALID_INTERVALS {
            manager.candles.insert(interval.to_string(), create_df());
        }

        manager
    }

    #[test]
    fn test_append_candles() {
        // create a data frame with 4 rows
        let df = create_df();

        // create a new data frame with 2 rows
        let new_df = polars::prelude::df!(
            "time" => &[3, 2],
            "open" => &[5.0, 6.0],
            "high" => &[5.0, 6.0],
            "low" => &[5.0, 6.0],
            "close" => &[5.0, 6.0],
            "volume" => &[5.0, 6.0]
        )
        .unwrap();

        // update the existing data frame with the new data frame
        let updated = super::append_candles(&df, new_df).unwrap();

        // assert that the updated data frame has 5 rows
        assert_eq!(updated.shape(), (5, 6));

        // assert that the third row contains values of 5.0, which came from the new data frame
        assert_eq!(
            updated
                .column("open")
                .unwrap()
                .f64()
                .unwrap()
                .get(3)
                .unwrap(),
            5.0
        );
    }

    #[test]
    fn test_save_candle_holder() {
        let suffix = Path::new(TEST_DIR).join("test_save_candles");
        let path = create_temp_dir(&suffix);

        // create some fake candle data
        let market = build_market();
        let mut manager = create_manager(&market);

        manager.save(&path).unwrap();

        // check that the files were created
        for i in VALID_INTERVALS.iter() {
            let file_path = path.join(format!("{}.csv", i));
            assert!(file_path.is_file());
        }

        // check the contents of each file
        let expected = create_df();

        for interval in VALID_INTERVALS.iter() {
            let file_path = path.join(format!("{}.csv", interval));
            let loaded = load_candles(&file_path).unwrap();
            assert_eq!(loaded.shape(), (4, 6));
            assert_eq!(loaded, expected);
        }

        // remove the temp dir
        remove_dir_all(&path).unwrap();
    }

    #[test]
    fn test_load_candle_holder() {
        let suffix = Path::new(TEST_DIR).join("test_load_candles");
        let path = create_temp_dir(&suffix);

        // create some fake candle data
        let market = build_market();
        let mut manager = create_manager(&market);

        manager.save(&path).unwrap();

        // load the candle holder
        let market = build_market();
        let mut loaded = create_manager(&market);
        loaded.load(&path).unwrap();

        // check that values are not None
        for interval in VALID_INTERVALS.iter() {
            assert!(loaded.candles.get(&interval.to_string()).is_some());
        }

        // check that there is the proper number of intervals
        assert_eq!(loaded.candles.len(), VALID_INTERVALS.len());

        // remove the temp dir
        remove_dir_all(&path).unwrap();
    }

    #[test]
    fn test_update_candles() {
        // create a data frame with 4 rows
        let df = create_df();

        // create a new data frame with 2 rows
        let new_df = df![
            "time" => &[3, 2],
            "open" => &[5.0, 6.0],
            "high" => &[5.0, 6.0],
            "low" => &[5.0, 6.0],
            "close" => &[5.0, 6.0],
            "volume" => &[5.0, 6.0]
        ]
        .unwrap();

        // update the existing data frame with the new data frame
        let updated = super::append_candles(&df, new_df).unwrap();

        // assert that the updated data frame has 5 rows
        assert_eq!(updated.shape(), (5, 6));

        // assert that the third row contains values of 5.0, which came from the new data frame
        assert_eq!(
            updated
                .column("open")
                .unwrap()
                .f64()
                .unwrap()
                .get(3)
                .unwrap(),
            5.0
        );
    }
}
