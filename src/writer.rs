use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};

pub const PARQUET_BATCH_SIZE: usize = 8 * 1024;
pub const PARQUET_PAGE_SIZE: usize = 1024 * 1024;
pub const PARQUET_MAX_ROW_GROUP_SIZE: usize = 1024 * 1024;

pub struct FileMeta {}

pub fn new_parquet_writer<'a>(
    buf: &'a mut Vec<u8>,
    schema: &'a Arc<Schema>,
    // bloom_filter: &'a [String],
    // meta: &'a FileMeta,
) -> ArrowWriter<&'a mut Vec<u8>> {
    let writer_props = WriterProperties::builder()
        .set_write_batch_size(PARQUET_BATCH_SIZE)
        .set_data_page_size_limit(PARQUET_PAGE_SIZE)
        .set_max_row_group_size(PARQUET_MAX_ROW_GROUP_SIZE);

    let writer_props = writer_props.build();
    ArrowWriter::try_new(buf, schema.clone(), Some(writer_props)).unwrap()
}
