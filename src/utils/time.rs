use chrono::NaiveDateTime;
use chrono::{DateTime, TimeZone, Utc};
use once_cell::sync::Lazy;
use serde_json::Value;

// BASE_TIME is the time when the timestamp is 1 year, used to check a timestamp
// is in seconds or milliseconds or microseconds or nanoseconds
pub static BASE_TIME: Lazy<DateTime<Utc>> =
    Lazy::new(|| Utc.with_ymd_and_hms(1971, 1, 1, 0, 0, 0).unwrap());

pub fn parse_timestamp(field_name: &str, val: &Value) -> Result<i64, anyhow::Error> {
    match val.get(field_name) {
        Some(v) => parse_timestamp_micro_from_value(v),
        None => Ok(Utc::now().timestamp_micros()),
    }
}

#[inline(always)]
pub fn parse_timestamp_micro_from_value(v: &Value) -> Result<i64, anyhow::Error> {
    let n = match v {
        Value::String(s) => parse_str_to_timestamp_micros(s)?,
        Value::Number(n) => {
            if n.is_i64() {
                n.as_i64().unwrap()
            } else if n.is_u64() {
                n.as_u64().unwrap() as i64
            } else if n.is_f64() {
                n.as_f64().unwrap() as i64
            } else {
                return Err(anyhow::anyhow!("Invalid time format [timestamp]"));
            }
        }
        _ => return Err(anyhow::anyhow!("Invalid time format [type]")),
    };
    Ok(parse_i64_to_timestamp_micros(n))
}

#[inline(always)]
pub fn parse_i64_to_timestamp_micros(v: i64) -> i64 {
    if v == 0 {
        return Utc::now().timestamp_micros();
    }
    let mut duration = v;
    if duration > BASE_TIME.timestamp_nanos_opt().unwrap_or_default() {
        // nanoseconds
        duration /= 1000;
    } else if duration > BASE_TIME.timestamp_micros() {
        // microseconds
        // noop
    } else if duration > BASE_TIME.timestamp_millis() {
        // milliseconds
        duration *= 1000;
    } else {
        // seconds
        duration *= 1_000_000;
    }
    duration
}

#[inline(always)]
pub fn parse_str_to_timestamp_micros(v: &str) -> Result<i64, anyhow::Error> {
    match v.parse() {
        Ok(i) => Ok(parse_i64_to_timestamp_micros(i)),
        Err(_) => match parse_str_to_time(v) {
            Ok(v) => Ok(v.timestamp_micros()),
            Err(_) => Err(anyhow::anyhow!("invalid time format [string]")),
        },
    }
}

#[inline(always)]
pub fn parse_str_to_time(s: &str) -> Result<DateTime<Utc>, anyhow::Error> {
    if let Ok(v) = s.parse::<f64>() {
        let v = parse_i64_to_timestamp_micros(v as i64);
        return Ok(Utc.timestamp_nanos(v * 1000));
    }

    let ret = if s.contains(' ') && s.len() == 19 {
        let fmt = "%Y-%m-%d %H:%M:%S";
        NaiveDateTime::parse_from_str(s, fmt)?.and_utc()
    } else if s.contains('T') && !s.contains(' ') {
        if s.len() == 19 {
            let fmt = "%Y-%m-%dT%H:%M:%S";
            NaiveDateTime::parse_from_str(s, fmt)?.and_utc()
        } else {
            let t = DateTime::parse_from_rfc3339(s)?;
            t.into()
        }
    } else {
        let t = DateTime::parse_from_rfc2822(s)?;
        t.into()
    };
    Ok(ret)
}
