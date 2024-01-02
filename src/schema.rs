use std::{ops::Deref, sync::Arc};

use datafusion::arrow::datatypes::Schema;
use serde_json::Value;

use crate::fusion::schema;
#[derive(Clone, Debug)]
pub struct MeltSchema {
    pub schema: Arc<Schema>,
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
    let schema = schema::infer_schema(&vals)?;
    Ok(MeltSchema {
        schema: Arc::new(schema),
    })
}
