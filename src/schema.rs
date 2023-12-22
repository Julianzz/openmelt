use std::{ops::Deref, sync::Arc};

use arrow_json::reader::infer_json_schema_from_iterator;
use datafusion::arrow::datatypes::Schema;
use serde_json::Value;

pub struct MeltSchema {
    schema: Arc<Schema>,
}

impl AsRef<Arc<Schema>> for MeltSchema {
    fn as_ref(&self) -> &Arc<Schema> {
        &self.schema
    }
}

impl Deref for MeltSchema {
    type Target = Arc<Schema>;

    fn deref(&self) -> &Self::Target {
        &self.schema
    }
}

impl MeltSchema {
    pub fn serialize(&self) -> Result<Vec<u8>, anyhow::Error> {
        Ok(serde_json::to_vec(&self.schema)?)
    }
}

pub fn infer_schema(vals: &[Value]) -> Result<MeltSchema, anyhow::Error> {
    // let reader = BufReader::new(body.as_ref());
    let iter = vals.iter().map(Ok);
    let schema = infer_json_schema_from_iterator(iter)?;
    Ok(MeltSchema {
        schema: Arc::new(schema),
    })
}
