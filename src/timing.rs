/// Synchronize runtime execution with candle intervals
use chrono::{Duration, NaiveDateTime, Timelike, Utc};
use std::time::Duration as StdDuration;
use tokio::time::sleep;

pub async fn wait_until(interval: &str) {
    // determine time until next interval
    let now = Utc::now().naive_utc();
    let next = get_wait_time(now, interval);

    let duration = StdDuration::new(
        next.num_seconds() as u64,
        next.num_nanoseconds().unwrap() as u32,
    );
    sleep(duration).await;
}

fn get_wait_time(now: NaiveDateTime, interval: &str) -> Duration {
    let next_time = match interval {
        "1m" => {
            let remaining = if now.second() == 0 {
                60
            } else {
                60 - now.second()
            };
            now + Duration::seconds(remaining as i64)
        }
        "5m" => {
            let remaining = if now.minute() % 5 == 0 {
                5
            } else {
                5 - (now.minute() % 5)
            };
            let next = now + Duration::minutes(remaining as i64);
            next.with_second(0).unwrap()
        }
        "15m" => {
            let remaining = if now.minute() % 15 == 0 {
                15 + now.minute()
            } else {
                15 - (now.minute() % 15)
            };
            let next = now + Duration::minutes(remaining as i64);
            next.with_second(0).unwrap()
        }
        "1h" => {
            let remaining = if now.minute() == 0 {
                60
            } else {
                60 - (now.minute() % 60)
            };
            let next = now + Duration::minutes(remaining as i64);
            next.with_minute(0).unwrap().with_second(0).unwrap()
        }
        "6h" => {
            let remaining = if now.hour() % 6 == 0 {
                6 + now.hour()
            } else {
                6 - (now.hour() % 6)
            };
            let next = now + Duration::hours(remaining as i64);
            next.with_minute(0).unwrap().with_second(0).unwrap()
        }
        "1d" => {
            let remaining = if now.hour() == 0 {
                24
            } else {
                24 - (now.hour() % 24)
            };
            let next = now + Duration::hours(remaining as i64);
            next.with_minute(0).unwrap().with_second(0).unwrap()
        }
        _ => panic!("Invalid interval"),
    };
    next_time - now
}

#[cfg(test)]
mod tests {
    use crate::timing::get_wait_time;
    use chrono::{Duration, NaiveDate};

    #[test]
    fn test_get_wait_time_1m() {
        // test simple
        let time = NaiveDate::from_ymd_opt(2016, 7, 8)
            .unwrap()
            .and_hms_opt(9, 10, 11)
            .unwrap();

        let expected = Duration::seconds(49);

        assert_eq!(get_wait_time(time, "1m"), expected);

        // test overlapping
        let time = NaiveDate::from_ymd_opt(2016, 7, 8)
            .unwrap()
            .and_hms_opt(23, 59, 3)
            .unwrap();

        let expected = Duration::seconds(57);

        assert_eq!(get_wait_time(time, "1m"), expected);
    }

    #[test]
    fn test_get_wait_time_5m() {
        // test simple
        let time = NaiveDate::from_ymd_opt(2016, 7, 8)
            .unwrap()
            .and_hms_opt(9, 14, 11)
            .unwrap();

        let expected = Duration::seconds(49);

        assert_eq!(get_wait_time(time, "5m"), expected);

        // test with another minute
        let time = NaiveDate::from_ymd_opt(2016, 7, 8)
            .unwrap()
            .and_hms_opt(9, 13, 11)
            .unwrap();

        let expected = Duration::seconds(109);

        assert_eq!(get_wait_time(time, "5m"), expected);
    }

    #[test]
    fn test_get_wait_time_15m() {
        let time = NaiveDate::from_ymd_opt(2016, 7, 8)
            .unwrap()
            .and_hms_opt(9, 14, 11)
            .unwrap();

        let expected = Duration::seconds(49);

        assert_eq!(get_wait_time(time, "15m"), expected);

        let time = NaiveDate::from_ymd_opt(2016, 7, 8)
            .unwrap()
            .and_hms_opt(9, 58, 11)
            .unwrap();

        let expected = Duration::minutes(1) + Duration::seconds(49);

        assert_eq!(get_wait_time(time, "15m"), expected);
    }

    #[test]
    fn test_get_wait_time_1h() {
        let time = NaiveDate::from_ymd_opt(2016, 7, 8)
            .unwrap()
            .and_hms_opt(9, 14, 11)
            .unwrap();

        let expected = Duration::minutes(45) + Duration::seconds(49);

        assert_eq!(get_wait_time(time, "1h"), expected);

        let time = NaiveDate::from_ymd_opt(2016, 7, 8)
            .unwrap()
            .and_hms_opt(23, 59, 11)
            .unwrap();

        let expected = Duration::seconds(49);

        assert_eq!(get_wait_time(time, "1h"), expected);
    }

    #[test]
    fn test_get_wait_time_6h() {
        let time = NaiveDate::from_ymd_opt(2016, 7, 8)
            .unwrap()
            .and_hms_opt(9, 14, 11)
            .unwrap();

        let expected = Duration::hours(2) + Duration::minutes(45) + Duration::seconds(49);

        assert_eq!(get_wait_time(time, "6h"), expected);

        let time = NaiveDate::from_ymd_opt(2016, 7, 8)
            .unwrap()
            .and_hms_opt(23, 59, 11)
            .unwrap();

        let expected = Duration::seconds(49);

        assert_eq!(get_wait_time(time, "6h"), expected);
    }

    #[test]
    fn test_get_wait_time_1d() {
        let time = NaiveDate::from_ymd_opt(2016, 7, 8)
            .unwrap()
            .and_hms_opt(9, 14, 11)
            .unwrap();

        let expected = Duration::hours(14) + Duration::minutes(45) + Duration::seconds(49);

        assert_eq!(get_wait_time(time, "1d"), expected);

        let time = NaiveDate::from_ymd_opt(2016, 7, 8)
            .unwrap()
            .and_hms_opt(23, 59, 11)
            .unwrap();

        let expected = Duration::seconds(49);

        assert_eq!(get_wait_time(time, "1d"), expected);
    }
}
