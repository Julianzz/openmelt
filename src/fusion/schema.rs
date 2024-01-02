use arrow_schema::{DataType, Field, Fields};
use datafusion::arrow::datatypes::Schema;
use serde_json::Value;
use std::collections::{HashMap, HashSet};

pub fn merge_schema(schemas: &[&Schema]) -> Result<Schema, anyhow::Error> {
    let mut field_types: HashMap<&str, HashSet<DataType>> = HashMap::new();
    for s in schemas {
        for f in s.all_fields() {
            field_types
                .entry(f.name())
                .or_default()
                .insert(f.data_type().clone());
        }
    }

    generate_schema(&field_types)
}

pub fn infer_schema(vals: &[Value]) -> Result<Schema, anyhow::Error> {
    let mut field_types = HashMap::<&str, HashSet<DataType>>::new();
    for record in vals {
        match record {
            Value::Object(map) => {
                for (k, v) in map {
                    let hs = field_types.entry(k).or_default();
                    match v {
                        Value::Null => {}
                        Value::Number(n) => {
                            if n.is_i64() {
                                hs.insert(DataType::Int64);
                            } else {
                                hs.insert(DataType::Float64);
                            }
                        }
                        Value::Bool(_) => {
                            hs.insert(DataType::Boolean);
                        }
                        Value::String(_) => {
                            hs.insert(DataType::Utf8);
                        }
                        Value::Array(_) | Value::Object(_) => {
                            return Err(anyhow::anyhow!("not support array or object"));
                        }
                    }
                }
            }
            _ => {
                return Err(anyhow::anyhow!("only support object"));
            }
        }
    }

    Ok(generate_schema(&field_types)?)
}

fn generate_schema(
    fields_types: &HashMap<&str, HashSet<DataType>>,
) -> Result<Schema, anyhow::Error> {
    Ok(Schema::new(generate_fields(fields_types)?))
}

fn generate_fields(fields: &HashMap<&str, HashSet<DataType>>) -> Result<Fields, anyhow::Error> {
    fields
        .iter()
        .map(|(&k, v)| {
            Ok(Field::new(
                k.to_string(),
                coerce_data_type(v.into_iter())?,
                true,
            ))
        })
        .collect()
}

fn coerce_data_type<'a, I: Iterator<Item = &'a DataType>>(
    dt: I,
) -> Result<DataType, anyhow::Error> {
    let mut dt_iter = dt.into_iter().cloned();
    let dt_init = dt_iter.next().unwrap_or(DataType::Utf8);

    Ok(dt_iter.fold(dt_init, |l, r| match (l, r) {
        (DataType::Null, o) | (o, DataType::Null) => o,
        (DataType::Boolean, DataType::Boolean) => DataType::Boolean,
        (DataType::Int64, DataType::Int64) => DataType::Int64,
        (DataType::Float64, DataType::Float64)
        | (DataType::Float64, DataType::Int64)
        | (DataType::Int64, DataType::Float64) => DataType::Float64,
        _ => DataType::Utf8,
    }))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_infer_schema() {
        let datas = json!([{
            "a": 1,
            "b": "zhong",
            "c": 2,
        },
        {
            "a":"zhong",
            "b": 2,
            "d": 12.0,
        }
        ]);
        let datas = datas.as_array().unwrap();
        let schema = infer_schema(datas).unwrap();
        println!("{:?}", schema);
        assert_eq!(
            schema.field_with_name("a").unwrap().data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            schema.field_with_name("b").unwrap().data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            schema.field_with_name("c").unwrap().data_type(),
            &DataType::Int64
        );
        assert_eq!(
            schema.field_with_name("d").unwrap().data_type(),
            &DataType::Float64
        );
    }
}
