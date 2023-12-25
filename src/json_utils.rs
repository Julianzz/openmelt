use std::io::{BufRead, BufReader};

use bytes::Bytes;
use chrono::Utc;
use serde_json::Value;

use crate::utils::parse_timestamp_micro_from_value;

pub fn parse_timestamp(field_name: &str, val: &Value) -> Result<i64, anyhow::Error> {
    match val.get(field_name) {
        Some(v) => parse_timestamp_micro_from_value(v),
        None => Ok(Utc::now().timestamp_micros()),
    }
}

pub fn parse_lines(body: Bytes) -> Result<Vec<Value>, anyhow::Error> {
    let reader = BufReader::new(body.as_ref());
    let mut records = Vec::new();
    for line in reader.lines() {
        let line = line?;
        // log::info!("{}", line);
        if line.is_empty() {
            continue;
        }
        let value = serde_json::from_slice(line.as_bytes())?;
        // log::info!("{:?}", value);
        records.push(value);
    }
    Ok(records)
}

pub fn parse_json(body: Bytes) -> Result<Vec<Value>, anyhow::Error> {
    let val: Value = serde_json::from_slice(&body)?;
    let records = if let Value::Array(val) = val {
        val
    } else {
        vec![val]
    };
    Ok(records)
}

