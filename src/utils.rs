use std::env::temp_dir;
use std::fs::{create_dir_all, remove_dir_all};
use std::path::{Path, PathBuf};
use polars::frame::DataFrame;
use polars::prelude::{DataFrameJoinOps, JoinArgs, JoinType};

/// create temp dir for testing
pub fn create_temp_dir(dir: &Path) -> PathBuf {
    let temp_dir = temp_dir();
    let path = temp_dir.join(dir);

    // delete dir if it already exists
    if path.exists() {
        remove_dir_all(&path).unwrap();
    }
    create_dir_all(path.clone()).unwrap();
    path
}

/// Extract new rows from a time-series DataFrame
///
/// This performs an anti-join between two columns along the "time" column. The result is a DataFrame
/// with the rows that are in the `updated` DataFrame but not in the `data` DataFrame.
///
/// This function is used when extracting new candle data that has not been processed by the indicator,
/// and indicator data that has not been processed for signals. For example, if `updated` has two rows
/// (with times "1" and "2") and `data` has one row (with time "1"), then the result will be a DataFrame with
/// one row (corresponding to time "2"). Comparison is exclusively done on the "time" column and the content
/// of the other columns is ignored.
///
/// # Arguments
/// * `updated` - The DataFrame with the new rows
/// * `data` - The DataFrame with the old rows
///
/// # Returns
/// A DataFrame with the new rows from `updated`
pub fn extract_new_rows(updated: &DataFrame, data: &DataFrame) -> DataFrame {
    // perform an anti-join to get the new rows
    updated
        .join(data, ["time"], ["time"], JoinArgs::new(JoinType::Anti))
        .unwrap()
}


#[cfg(test)]
mod tests {
    use crate::utils::extract_new_rows;
    use polars::prelude::*;

    /// Test that extract_new_rows() returns the correct rows
    #[test]
    fn test_extract_new_rows() {
        let candles = df!(
            "time" => &[1, 2, 3, 41, 51],
            "open" => &[1, 2, 3, 42, 52],
            "high" => &[1, 2, 3, 43, 53],
            "low" => &[1, 2, 3, 44, 54],
            "close" => &[1, 2, 3, 45, 55],
            "volume" => &[1, 2, 3, 46, 56],
        )
            .unwrap();

        let indicator_data = df!(
            "time" => &[1, 2, 3],
            "open" => &[1, 2, 3],
            "high" => &[1, 2, 3],
            "low" => &[1, 2, 3],
            "close" => &[1, 2, 3],
            "volume" => &[1, 2, 3],
        )
            .unwrap();

        let new_rows = extract_new_rows(&candles, &indicator_data);

        assert_eq!(new_rows.shape(), (2, 6));

        // check time column
        assert_eq!(
            new_rows.column("time").unwrap().i32().unwrap().get(0),
            Some(41)
        );
        assert_eq!(
            new_rows.column("time").unwrap().i32().unwrap().get(1),
            Some(51)
        );

        // check open column
        assert_eq!(
            new_rows.column("open").unwrap().i32().unwrap().get(0),
            Some(42)
        );
        assert_eq!(
            new_rows.column("open").unwrap().i32().unwrap().get(1),
            Some(52)
        );

        assert_eq!(
            new_rows.column("high").unwrap().i32().unwrap().get(0),
            Some(43)
        );
        assert_eq!(
            new_rows.column("high").unwrap().i32().unwrap().get(1),
            Some(53)
        );

        assert_eq!(
            new_rows.column("low").unwrap().i32().unwrap().get(0),
            Some(44)
        );
        assert_eq!(
            new_rows.column("low").unwrap().i32().unwrap().get(1),
            Some(54)
        );

        assert_eq!(
            new_rows.column("close").unwrap().i32().unwrap().get(0),
            Some(45)
        );
        assert_eq!(
            new_rows.column("close").unwrap().i32().unwrap().get(1),
            Some(55)
        );

        assert_eq!(
            new_rows.column("volume").unwrap().i32().unwrap().get(0),
            Some(46)
        );
        assert_eq!(
            new_rows.column("volume").unwrap().i32().unwrap().get(1),
            Some(56)
        );
    }
}
