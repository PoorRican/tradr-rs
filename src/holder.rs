use crate::traits::AsDataFrame;
use crate::types::Candle;
use polars::error::PolarsResult;
use polars::frame::{DataFrame, UniqueKeepStrategy};
use polars_io::prelude::{CsvReader, CsvWriter};
use polars_io::{SerReader, SerWriter};
use std::fs::OpenOptions;
use std::io::Error;
use std::path::Path;

const ONE_MINUTE_FN: &str = "1m.csv";
const FIVE_MINUTES_FN: &str = "5m.csv";
const FIFTEEN_MINUTES_FN: &str = "15m.csv";
const THIRTY_MINUTES_FN: &str = "30m.csv";
const ONE_HOUR_FN: &str = "1h.csv";
const SIX_HOURS_FN: &str = "6h.csv";
const DAILY_FN: &str = "daily.csv";

struct CandleHolder {
    pub one_minute: Option<DataFrame>,
    pub five_minutes: Option<DataFrame>,
    pub fifteen_minutes: Option<DataFrame>,
    pub thirty_minutes: Option<DataFrame>,
    pub one_hour: Option<DataFrame>,
    pub six_hours: Option<DataFrame>,
    pub daily: Option<DataFrame>,
}

impl CandleHolder {
    pub fn new() -> Self {
        Self {
            one_minute: None,
            five_minutes: None,
            fifteen_minutes: None,
            thirty_minutes: None,
            one_hour: None,
            six_hours: None,
            daily: None,
        }
    }

    pub fn set_1m(mut self, candles: DataFrame) -> Self {
        self.one_minute = Some(candles);
        self
    }

    pub fn set_5m(mut self, candles: DataFrame) -> Self {
        self.five_minutes = Some(candles);
        self
    }

    pub fn set_15m(mut self, candles: DataFrame) -> Self {
        self.fifteen_minutes = Some(candles);
        self
    }

    pub fn set_30m(mut self, candles: DataFrame) -> Self {
        self.thirty_minutes = Some(candles);
        self
    }

    pub fn set_1h(mut self, candles: DataFrame) -> Self {
        self.one_hour = Some(candles);
        self
    }

    pub fn set_6h(mut self, candles: DataFrame) -> Self {
        self.six_hours = Some(candles);
        self
    }

    pub fn set_daily(mut self, candles: DataFrame) -> Self {
        self.daily = Some(candles);
        self
    }

    pub fn update_1m(&mut self, candles: Vec<Candle>) -> PolarsResult<()> {
        let new_candles = candles.as_dataframe();
        match &self.one_minute {
            Some(existing) => {
                let updated = append_candles(existing, new_candles);
                self.one_minute = Some(updated?);
            }
            None => {
                self.one_minute = Some(new_candles);
            }
        }
        Ok(())
    }

    pub fn update_5m(&mut self, candles: Vec<Candle>) -> PolarsResult<()> {
        let new_candles = candles.as_dataframe();
        match &self.five_minutes {
            Some(existing) => {
                let updated = append_candles(existing, new_candles);
                self.five_minutes = Some(updated?);
            }
            None => {
                self.five_minutes = Some(new_candles);
            }
        }
        Ok(())
    }

    pub fn update_15m(&mut self, candles: Vec<Candle>) -> PolarsResult<()> {
        let new_candles = candles.as_dataframe();
        match &self.fifteen_minutes {
            Some(existing) => {
                let updated = append_candles(existing, new_candles);
                self.fifteen_minutes = Some(updated?);
            }
            None => {
                self.fifteen_minutes = Some(new_candles);
            }
        }
        Ok(())
    }

    pub fn update_30m(&mut self, candles: Vec<Candle>) -> PolarsResult<()> {
        let new_candles = candles.as_dataframe();
        match &self.thirty_minutes {
            Some(existing) => {
                let updated = append_candles(existing, new_candles);
                self.thirty_minutes = Some(updated?);
            }
            None => {
                self.thirty_minutes = Some(new_candles);
            }
        }
        Ok(())
    }

    pub fn update_1h(&mut self, candles: Vec<Candle>) -> PolarsResult<()> {
        let new_candles = candles.as_dataframe();
        match &self.one_hour {
            Some(existing) => {
                let updated = append_candles(existing, new_candles);
                self.one_hour = Some(updated?);
            }
            None => {
                self.one_hour = Some(new_candles);
            }
        }
        Ok(())
    }

    pub fn update_6h(&mut self, candles: Vec<Candle>) -> PolarsResult<()> {
        let new_candles = candles.as_dataframe();
        match &self.six_hours {
            Some(existing) => {
                let updated = append_candles(existing, new_candles);
                self.six_hours = Some(updated?);
            }
            None => {
                self.six_hours = Some(new_candles);
            }
        }
        Ok(())
    }

    pub fn update_daily(&mut self, candles: Vec<Candle>) -> PolarsResult<()> {
        let new_candles = candles.as_dataframe();
        match &self.daily {
            Some(existing) => {
                let updated = append_candles(existing, new_candles);
                self.daily = Some(updated?);
            }
            None => {
                self.daily = Some(new_candles);
            }
        }
        Ok(())
    }

    pub fn save(&mut self, path: &Path) -> Result<(), Error> {
        if !path.is_dir() {
            return Err(Error::new(
                std::io::ErrorKind::InvalidInput,
                "path must be a directory",
            ));
        }

        let file_names = [
            ONE_MINUTE_FN,
            FIVE_MINUTES_FN,
            FIFTEEN_MINUTES_FN,
            THIRTY_MINUTES_FN,
            ONE_HOUR_FN,
            SIX_HOURS_FN,
            DAILY_FN,
        ];

        let mut data_frames = [
            &mut self.one_minute,
            &mut self.five_minutes,
            &mut self.fifteen_minutes,
            &mut self.thirty_minutes,
            &mut self.one_hour,
            &mut self.six_hours,
            &mut self.daily,
        ];

        for (file_name, data_frame) in file_names.iter().zip(data_frames.iter_mut()) {
            if let Some(df) = data_frame {
                let file_path = path.join(file_name);
                save_candles(&file_path, df)?;
            }
        }

        Ok(())
    }

    pub fn load(&mut self, path: &Path) -> Result<(), Error> {
        if !path.is_dir() {
            return Err(Error::new(
                std::io::ErrorKind::InvalidInput,
                "path must be a directory",
            ));
        }

        let file_names = [
            ONE_MINUTE_FN,
            FIVE_MINUTES_FN,
            FIFTEEN_MINUTES_FN,
            THIRTY_MINUTES_FN,
            ONE_HOUR_FN,
            SIX_HOURS_FN,
            DAILY_FN,
        ];

        let mut data_frames = [
            &mut self.one_minute,
            &mut self.five_minutes,
            &mut self.fifteen_minutes,
            &mut self.thirty_minutes,
            &mut self.one_hour,
            &mut self.six_hours,
            &mut self.daily,
        ];

        for (file_name, data_frame) in file_names.iter().zip(data_frames.iter_mut()) {
            let file_path = path.join(file_name);
            if file_path.is_file() {
                let df = load_candles(&file_path)?;
                **data_frame = Some(df);
            }
        }

        Ok(())
    }
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

#[cfg(test)]
mod tests {
    use crate::holder::{
        load_candles, CandleHolder, DAILY_FN, FIFTEEN_MINUTES_FN, FIVE_MINUTES_FN, ONE_HOUR_FN,
        ONE_MINUTE_FN, SIX_HOURS_FN, THIRTY_MINUTES_FN,
    };
    use crate::utils::create_temp_dir;
    use polars::prelude::*;
    use std::fs::remove_dir_all;
    use std::path::Path;

    const TEST_DIR: &str = "candle_holder_testing";

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

    fn create_holder() -> (DataFrame, CandleHolder) {
        let df = create_df();

        // create a candle holder with the fake data then save
        let mut holder = CandleHolder::new()
            .set_1m(df.clone())
            .set_5m(df.clone())
            .set_15m(df.clone())
            .set_30m(df.clone())
            .set_1h(df.clone())
            .set_6h(df.clone())
            .set_daily(df.clone());

        (df, holder)
    }

    #[test]
    fn test_save_candle_holder() {
        let suffix = Path::new(TEST_DIR).join("test_save_candles");
        let path = create_temp_dir(&suffix);

        // create some fake candle data
        let (df, mut holder) = create_holder();

        holder.save(&path).unwrap();

        // check that the files were created
        assert!(path.join(ONE_MINUTE_FN).is_file());
        assert!(path.join(FIVE_MINUTES_FN).is_file());
        assert!(path.join(FIFTEEN_MINUTES_FN).is_file());
        assert!(path.join(THIRTY_MINUTES_FN).is_file());
        assert!(path.join(ONE_HOUR_FN).is_file());
        assert!(path.join(SIX_HOURS_FN).is_file());
        assert!(path.join(DAILY_FN).is_file());

        // check the contents of each file
        let one_minute = load_candles(&path.join(ONE_MINUTE_FN)).unwrap();
        assert_eq!(one_minute.shape(), (4, 6));
        assert_eq!(one_minute, df);

        let five_minutes = load_candles(&path.join(FIVE_MINUTES_FN)).unwrap();
        assert_eq!(five_minutes.shape(), (4, 6));
        assert_eq!(five_minutes, df);

        let fifteen_minutes = load_candles(&path.join(FIFTEEN_MINUTES_FN)).unwrap();
        assert_eq!(fifteen_minutes.shape(), (4, 6));
        assert_eq!(fifteen_minutes, df);

        let thirty_minutes = load_candles(&path.join(THIRTY_MINUTES_FN)).unwrap();
        assert_eq!(thirty_minutes.shape(), (4, 6));
        assert_eq!(thirty_minutes, df);

        let one_hour = load_candles(&path.join(ONE_HOUR_FN)).unwrap();
        assert_eq!(one_hour.shape(), (4, 6));
        assert_eq!(one_hour, df);

        let six_hours = load_candles(&path.join(SIX_HOURS_FN)).unwrap();
        assert_eq!(six_hours.shape(), (4, 6));
        assert_eq!(six_hours, df);

        let daily = load_candles(&path.join(DAILY_FN)).unwrap();
        assert_eq!(daily.shape(), (4, 6));
        assert_eq!(daily, df);

        // remove the temp dir
        remove_dir_all(&path).unwrap();
    }

    #[test]
    fn test_load_candle_holder() {
        let suffix = Path::new(TEST_DIR).join("test_load_candles");
        let path = create_temp_dir(&suffix);

        // create some fake candle data
        let (df, mut holder) = create_holder();

        holder.save(&path).unwrap();

        // load the candle holder
        let mut loaded = CandleHolder::new();
        loaded.load(&path).unwrap();

        // check that values are not None
        assert!(loaded.one_minute.is_some());
        assert!(loaded.five_minutes.is_some());
        assert!(loaded.fifteen_minutes.is_some());
        assert!(loaded.thirty_minutes.is_some());
        assert!(loaded.one_hour.is_some());
        assert!(loaded.six_hours.is_some());
        assert!(loaded.daily.is_some());

        // check that the values are the same as the original
        assert_eq!(loaded.one_minute.unwrap(), df);
        assert_eq!(loaded.five_minutes.unwrap(), df);
        assert_eq!(loaded.fifteen_minutes.unwrap(), df);
        assert_eq!(loaded.thirty_minutes.unwrap(), df);
        assert_eq!(loaded.one_hour.unwrap(), df);
        assert_eq!(loaded.six_hours.unwrap(), df);
        assert_eq!(loaded.daily.unwrap(), df);

        // remove the temp dir
        remove_dir_all(&path).unwrap();
    }

    #[test]
    fn test_update_candles() {
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
}
