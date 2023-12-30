/// Custom serializer/deserializer for NaiveDateTime
///
/// This is a workaround for the fact that Serde does not support serializing or deserializing
/// into a struct with `NaiveDateTime` fields.
use chrono::NaiveDateTime;
use serde::{Deserialize, Serializer};

#[allow(dead_code)]
pub fn naive_dt_serializer<S>(dt: &NaiveDateTime, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_i64(dt.timestamp())
}

#[allow(dead_code)]
pub fn naive_dt_deserializer<'de, D>(deserializer: D) -> Result<NaiveDateTime, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let timestamp = i64::deserialize(deserializer)?;
    Ok(NaiveDateTime::from_timestamp_opt(timestamp, 0).unwrap())
}
