use serde_json::Value;

static BULK_OPERATIONS: [&str; 3] = ["create", "index", "update"];

pub fn parse_bulk_index(v: &Value) -> Option<(String, String, String)> {
    if let Some(local) = v.as_object() {
        for action in BULK_OPERATIONS {
            if !local.contains_key(action) {
                continue;
            }
            if let Some(val) = local.get(action).unwrap().as_object() {
                let index = match val.get("_index") {
                    Some(v) => v.as_str().unwrap().to_string(),
                    None => return None,
                };
                let doc_id = match val.get("_id") {
                    Some(v) => v.as_str().unwrap().to_string(),
                    None => return None,
                };
                return Some((action.to_owned(), index, doc_id));
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_bulk() {
        let data = r#"{ "create": {"_index":"action", "_id": "23" } }"#;
        let v = serde_json::from_str(data).unwrap();
        if let Some((action, index, id)) = parse_bulk_index(&v) {
            assert_eq!(action, "create");
            assert_eq!(index, "action");
            assert_eq!(id, "23")
        } else {
            panic!("error in parse")
        }
    }
}
