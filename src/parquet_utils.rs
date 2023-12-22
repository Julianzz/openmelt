use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};

// TODO to optimize
pub fn new_parquet_writer<'a>(
    dest: &'a mut Vec<u8>,
    schema: &'a Arc<Schema>,
) -> Result<ArrowWriter<&'a mut Vec<u8>>, anyhow::Error> {
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_dictionary_enabled(false)
        .build();
    Ok(ArrowWriter::try_new(dest, schema.clone(), Some(props))?)
}
