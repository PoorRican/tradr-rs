/// Ability to save and load portfolios from disk.

use csv::{Writer, Reader};
use std::fs::OpenOptions;
use std::io::Error;
use std::path::Path;
use chrono::NaiveDateTime;
use polars::prelude::*;

use crate::portfolio::Portfolio;
use crate::portfolio::tracked::TrackedValue;

const EXECUTED_TRADES_FILENAME: &str = "executed_trades.csv";
const FAILED_TRADES_FILENAME: &str = "failed_trades.csv";
const OPEN_POSITIONS_FILENAME: &str = "open_positions.csv";
const CAPITAL_FILENAME: &str = "capital.csv";
const ASSETS_FILENAME: &str = "assets.csv";

const DATETIME_FORMAT: &str = "%Y-%m-%dT%H:%M:%S";


/// Introduces the ability to save and load portfolios from disk.
///
/// Portfolio will not be given the functionality of managing the specific
/// instance directory. This is left to the object which initializes the object.
/// The intention is to have a higher-level object which links the portfolio
/// to other objects, such as a strategy, and manages the directory. That way
/// multiple portfolios can be managed by a single object.
pub trait Persistence {
    /// Save the portfolio to disk
    ///
    /// # Arguments
    /// * `path` - The path to the directory in which to save the portfolio
    ///
    /// # Errors
    /// * If the path is not a directory
    /// * If there are any IO errors
    fn save(&mut self, path: &Path) -> Result<(), Error>;

    /// Load a portfolio from disk
    ///
    /// # Arguments
    /// * `path` - The path to the directory in which to save the portfolio
    ///
    /// # Errors
    /// * If the path is not a directory
    /// * If there are any parsing errors
    /// * If there are any IO errors
    fn load(path: &Path) -> Result<Self, Error> where Self: Sized;
}

impl Persistence for Portfolio {
    fn save(&mut self, path: &Path) -> Result<(), Error> {
        if !path.is_dir() {
            return Err(Error::new(
                std::io::ErrorKind::InvalidInput,
                "path must be a directory"
            ));
        }

        // save executed trades into csv
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(path.join(EXECUTED_TRADES_FILENAME))?;
        CsvWriter::new(file)
            .include_header(true)
            .finish(&mut self.executed_trades).unwrap();

        // save failed trades
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(path.join(FAILED_TRADES_FILENAME))?;
        CsvWriter::new(file)
            .include_header(true)
            .finish(&mut self.failed_trades).unwrap();

        // save open positions
        let file_path = path.join(OPEN_POSITIONS_FILENAME);
        let mut wtr = Writer::from_path(file_path)?;

        wtr.write_record(&["timestamp"])?;
        for item in self.open_positions.iter() {
            wtr.write_record(&[item.format(DATETIME_FORMAT).to_string()])?;
        }
        wtr.flush()?;

        // save capital
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(path.join(CAPITAL_FILENAME))?;
        CsvWriter::new(file)
            .include_header(true)
            .finish(&mut self.capital_ts.clone().into()).unwrap();

        // save assets
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(path.join(ASSETS_FILENAME))?;
        CsvWriter::new(file)
            .include_header(true)
            .finish(&mut self.assets_ts.clone().into()).unwrap();

        Ok(())
    }

    fn load(path: &Path) -> Result<Self, Error> {
        if !path.is_dir() {
            return Err(Error::new(
                std::io::ErrorKind::InvalidInput,
                "path must be a directory"
            ));
        }

        // load executed trades
        let file_path = path.join(EXECUTED_TRADES_FILENAME);
        let mut executed_trades =
            CsvReader::from_path(file_path)
                .unwrap()
                .has_header(true)
                .with_try_parse_dates(true)
                .finish()
                .unwrap();

        // point column needs to be recasted because it is automatically parsed as microseconds
        let casted = executed_trades.column("point").unwrap().cast(&DataType::Datetime(TimeUnit::Milliseconds, None)).unwrap();
        executed_trades.with_column(casted).unwrap();

        // load failed trades
        let file_path = path.join(FAILED_TRADES_FILENAME);
        let mut failed_trades =
            CsvReader::from_path(file_path)
                .unwrap()
                .has_header(true)
                .with_try_parse_dates(true)
                .finish()
                .unwrap();

        // point column needs to be casted because it is automatically parsed as microseconds
        let casted = failed_trades.column("point").unwrap().cast(&DataType::Datetime(TimeUnit::Milliseconds, None)).unwrap();
        failed_trades.with_column(casted).unwrap();

        // load open positions
        let file_path = path.join(OPEN_POSITIONS_FILENAME);
        let mut rdr = Reader::from_path(file_path)?;
        let mut open_positions = Vec::new();
        for result in rdr.records() {
            let record = result?;
            let point =
                NaiveDateTime::parse_from_str(&record[0], DATETIME_FORMAT)
                    .unwrap();
            open_positions.push(point);
        }

        // load capital
       let file_path = path.join(CAPITAL_FILENAME);
        let capital_ts = TrackedValue::from(
            CsvReader::from_path(file_path)
                .unwrap()
                .has_header(true)
                .with_try_parse_dates(true)
                .finish()
                .unwrap()
        );


        // load assets
        let file_path = path.join(ASSETS_FILENAME);
        let assets_ts = TrackedValue::from(
            CsvReader::from_path(file_path)
                .unwrap()
                .has_header(true)
                .with_try_parse_dates(true)
                .finish()
                .unwrap()
        );

        // create the portfolio from the loaded data
        let portfolio = Portfolio::with_data(
            failed_trades,
            executed_trades,
            open_positions,
            assets_ts,
            capital_ts,
        );

        Ok(portfolio)
    }
}

#[cfg(test)]
mod tests {
    use std::fs::create_dir;
    use std::env::temp_dir;
    use std::fs::remove_dir_all;

    const TEST_DIR: &str = "portfolio_testing";

    fn create_temp_dir(suffix: &str) {

        let temp_dir = temp_dir();
        let path = temp_dir.join(TEST_DIR).join(suffix);

        // delete dir if it already exists
        if path.exists() {
            remove_dir_all(&path).unwrap();
        }
        create_dir(path).unwrap();
    }

    fn remove_temp_dir(suffix: &str) {

        let temp_dir = temp_dir();
        let path = temp_dir.join(TEST_DIR).join(suffix);
        remove_dir_all(path).unwrap();
    }

    use crate::portfolio::{AssetHandlers, CapitalHandlers, PositionHandlers, TradeHandlers};
    use crate::types::{ExecutedTrade, FailedTrade, ReasonCode, Side};
    use super::*;

    #[test]
    fn test_save() {
        use std::fs::read_dir;
        use std::io::Read;
        use std::env::temp_dir;

        let suffix = "save";
        create_temp_dir(suffix);

        let time = NaiveDateTime::from_timestamp_opt(0, 0).unwrap();

        let mut portfolio = Portfolio::new(100.0, 100.0, time);
        portfolio.add_executed_trade(
            ExecutedTrade::new_without_cost(
                "test_id".to_string(),
                Side::Buy,
                100.0,
                1.0,
                time + chrono::Duration::seconds(1)
            )
        );
        portfolio.add_failed_trade(
            FailedTrade::new(
                ReasonCode::Unknown,
                Side::Buy,
                100.0,
                1.0,
                time + chrono::Duration::seconds(1)
            )
        );

        let temp_dir = temp_dir();
        let path = temp_dir.join(TEST_DIR).join(suffix);

        portfolio.save(&path).unwrap();

        let mut files = Vec::new();
        for entry in read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let mut file = OpenOptions::new()
                .read(true)
                .open(entry.path())
                .unwrap();
            let mut contents = String::new();
            file.read_to_string(&mut contents).unwrap();
            files.push(contents);
        }

        let expected_files = vec![
            "timestamp,value\n1970-01-01T00:00:00.000,100.0\n1970-01-01T00:00:01.000,0.0\n",
            "timestamp,value\n1970-01-01T00:00:00.000,100.0\n1970-01-01T00:00:01.000,101.0\n",
            "side,price,quantity,cost,reason,point\n1,100.0,1.0,100.0,0,1970-01-01T00:00:01.000\n",
            "timestamp\n1970-01-01T00:00:01\n",
            "id,side,price,quantity,cost,point\ntest_id,1,100.0,1.0,100.0,1970-01-01T00:00:01.000\n",
        ];

        assert_eq!(files, expected_files);

        remove_temp_dir(suffix);
    }

    /// Ensure that the save function does not panic when the files already exist
    #[test]
    fn test_save_when_existing() {
        use std::env::temp_dir;

        let suffix = "save_when_existing";
        create_temp_dir(suffix);

        let time = NaiveDateTime::from_timestamp_opt(0, 0).unwrap();

        let mut portfolio = Portfolio::new(100.0, 100.0, time);
        portfolio.add_executed_trade(
            ExecutedTrade::new_without_cost(
                "test_id".to_string(),
                Side::Buy,
                100.0,
                1.0,
                time + chrono::Duration::seconds(1)
            )
        );
        portfolio.add_failed_trade(
            FailedTrade::new(
                ReasonCode::Unknown,
                Side::Buy,
                100.0,
                1.0,
                time + chrono::Duration::seconds(1)
            )
        );

        let temp_dir = temp_dir();
        let path = temp_dir.join(TEST_DIR).join(suffix);

        portfolio.save(&path).unwrap();
        portfolio.save(&path).unwrap();

        remove_temp_dir(suffix);
    }

    #[test]
    #[should_panic]
    fn test_save_invalid_path() {
        let time = NaiveDateTime::from_timestamp_opt(0, 0).unwrap();
        let mut portfolio = Portfolio::new(100.0, 100.0, time);
        portfolio.save(Path::new("invalid_path")).unwrap();
    }

    #[test]
    fn test_load() {
        use std::env::temp_dir;

        let suffix = "load";
        create_temp_dir(suffix);

        let time = NaiveDateTime::from_timestamp_opt(0, 0).unwrap();

        let mut portfolio = Portfolio::new(100.0, 100.0, time);
        portfolio.add_executed_trade(
            ExecutedTrade::new_without_cost(
                "test_id".to_string(),
                Side::Buy,
                100.0,
                1.0,
                time + chrono::Duration::seconds(1)
            )
        );
        portfolio.add_failed_trade(
            FailedTrade::new(
                ReasonCode::Unknown,
                Side::Buy,
                100.0,
                1.0,
                time + chrono::Duration::seconds(1)
            )
        );
        assert_eq!(portfolio.get_open_positions().unwrap().height(), 1);

        let temp_dir = temp_dir();
        let path = temp_dir.join(TEST_DIR).join(suffix);
        portfolio.save(&path).unwrap();

        let portfolio = Portfolio::load(&path).unwrap();

        // check assets and capital
        assert_eq!(portfolio.get_assets(), 101.0);
        let df: DataFrame = portfolio.assets_ts.clone().into();
        assert_eq!(df.height(), 2);

        assert_eq!(portfolio.get_capital(), 0.0);
        let df: DataFrame = portfolio.capital_ts.clone().into();
        assert_eq!(df.height(), 2);

        // check executed and failed trades
        let expected_time = time + chrono::Duration::seconds(1);

        assert_eq!(portfolio.get_executed_trades().height(), 1);
        assert_eq!(portfolio.executed_trades.column("point").unwrap().datetime().unwrap().get(0).unwrap(), expected_time.timestamp_millis());

        assert_eq!(portfolio.failed_trades.height(), 1);
        assert_eq!(portfolio.failed_trades.column("point").unwrap().datetime().unwrap().get(0).unwrap(), expected_time.timestamp_millis());

        // check open positions
        assert_eq!(portfolio.get_open_positions().unwrap().height(), 1);
        assert_eq!(portfolio.open_positions.get(0).unwrap(), &expected_time);

        remove_temp_dir(suffix);
    }

    #[test]
    #[should_panic]
    fn test_load_invalid_path() {
        Portfolio::load(Path::new("invalid_path")).unwrap();
    }
}