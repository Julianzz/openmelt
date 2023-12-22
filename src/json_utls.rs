use chrono::Utc;
use serde_json::Value;

use crate::utils::parse_timestamp_micro_from_value;

pub fn parse_timestamp(field_name: &str, val: &Value) -> Result<i64, anyhow::Error> {
    match val.get(field_name) {
        Some(v) => parse_timestamp_micro_from_value(v),
        None => Ok(Utc::now().timestamp_micros()),
    }
}
