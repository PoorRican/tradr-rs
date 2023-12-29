use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Error;
use std::path::Path;
use polars::error::PolarsResult;
use polars::frame::{DataFrame, UniqueKeepStrategy};
use polars_io::csv::{CsvReader, CsvWriter};
use polars_io::{SerReader, SerWriter};
use crate::markets::BaseMarket;
use crate::traits::AsDataFrame;


const VALID_INTERVALS: [&str; 6] = ["1m", "5m", "15m", "1h", "6h", "1d"];


/// Updates the existing data frame with the new data frame.
///
/// Any rows that have the same time value will be overwritten.
///
/// # Arguments
/// * `existing` - Reference to the existing data frame.
/// * `new_candles` - The new data frame to update the existing data frame with.
///
/// # Returns
/// * `DataFrame` - The updated data frame with new candles
fn append_candles(existing: &DataFrame, new_candles: DataFrame) -> PolarsResult<DataFrame> {
    let mut appended = existing.vstack(&new_candles)?;

    appended.unique_stable(Some(&["time".to_string()]), UniqueKeepStrategy::Last, None)
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

    let df = CsvReader::from_path(file_path)
        .unwrap()
        .has_header(true)
        .with_try_parse_dates(true)
        .finish()
        .unwrap();
    Ok(df)
}


struct CandleManager {
    candles: HashMap<String, DataFrame>,
    pair: String,
    market: Box<dyn BaseMarket>,
}

impl CandleManager {
    pub fn new(pair: &str, market: Box<dyn BaseMarket>) -> Self {
        Self {
            candles: HashMap::new(),
            pair: pair.to_string(),
            market,
        }
    }

    pub fn get(&self, interval: &str) -> Option<&DataFrame> {
        self.candles.get(&interval.to_string())
    }

    pub async fn update(&mut self, interval: &str) {
        let candles = self.market.get_candles(&self.pair, interval)
            .await
            .unwrap();
        let df = candles.as_dataframe();
        match self.candles.get(interval) {
            Some(existing) => {
                let updated = append_candles(existing, df).unwrap();
                self.candles.insert(interval.to_string(), updated);
            }
            None => {
                self.candles.insert(interval.to_string(), df);
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
    use std::fs::remove_dir_all;
    use std::path::Path;
    use polars::prelude::*;
    use polars::frame::DataFrame;
    use crate::markets::CoinbaseClient;
    use crate::markets::manager::{CandleManager, load_candles, VALID_INTERVALS};
    use crate::utils::create_temp_dir;

    const TEST_DIR: &str = "candle_manager_testing";

    fn create_df() -> DataFrame {
        df!(
            "time" => &[1, 2, 3, 4],
            "open" => &[1.0, 2.0, 3.0, 4.0],
            "high" => &[1.0, 2.0, 3.0, 4.0],
            "low" => &[1.0, 2.0, 3.0, 4.0],
            "close" => &[1.0, 2.0, 3.0, 4.0],
            "volume" => &[1.0, 2.0, 3.0, 4.0]
        )
            .unwrap()
    }

    fn create_manager() -> CandleManager {
        let market = Box::new(CoinbaseClient::new());
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
            "time" => &[4, 5],
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
        let df = create_df();
        let mut holder = create_manager();

        holder.save(&path).unwrap();

        // check that the files were created
        for i in VALID_INTERVALS.iter() {
            let file_path = path.join(format!("{}.csv", i));
            assert!(file_path.is_file());
        }

        // check the contents of each file
        for interval in VALID_INTERVALS.iter() {
            let file_path = path.join(format!("{}.csv", interval));
            let loaded = load_candles(&file_path).unwrap();
            assert_eq!(loaded.shape(), (4, 6));
            assert_eq!(loaded, df);
        }

        // remove the temp dir
        remove_dir_all(&path).unwrap();
    }

    #[test]
    fn test_load_candle_holder() {
        let suffix = Path::new(TEST_DIR).join("test_load_candles");
        let path = create_temp_dir(&suffix);

        // create some fake candle data
        let df = create_df();
        let mut holder = create_manager();

        holder.save(&path).unwrap();

        // load the candle holder
        let mut loaded = create_manager();
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
            "time" => &[4, 5],
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