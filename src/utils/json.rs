use std::io::{BufRead, BufReader};

use bytes::Bytes;
use serde_json::{Map, Value};

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

pub fn flatten_json(val: &Value) -> Result<Value, anyhow::Error> {
    let mut res = Map::<String, Value>::new();
    if let Some(current) = val.as_object() {
        for (key, val) in current.iter() {
            flatten_value(key, val, &mut res)?;
        }
    } else {
        return Err(anyhow::anyhow!("json value should be map"));
    }
    Ok(Value::Object(res))
}

fn flatten_value(
    prefix: &str,
    current: &Value,
    res: &mut Map<String, Value>,
) -> Result<(), anyhow::Error> {
    match current {
        v @ Value::Array(_) => {
            res.insert(
                prefix.to_string(),
                Value::String(serde_json::to_string(v).unwrap()),
            );
        }
        Value::Object(map) => {
            // emit empty
            for (k, v) in map {
                flatten_value(&format!("{}.{}", prefix, k), v, res)?;
            }
        }
        // emit null
        Value::Null => {}
        // default value
        _ => {
            res.insert(prefix.to_string(), current.clone());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_flatten_json() {
        let data = json!({
            "a":{
                "b":1,
                "c":"time"
            },
            "c":2,
            "d": [1,2,4]
        });

        let f = flatten_json(&data).unwrap();
        println!("{}", f);
        assert_eq!(f.get("a.b"), Some(&Value::from(1)));
        assert_eq!(f.get("a.c"), Some(&Value::from("time")));
        assert_eq!(f.get("c"), Some(&Value::from(2)));
        assert_eq!(f.get("d"), Some(&Value::from("[1,2,4]")));
    }
}
