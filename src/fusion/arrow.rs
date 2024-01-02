use arrow_ipc::writer::StreamWriter;
use datafusion::arrow::{
    datatypes::Schema,
    record_batch::{RecordBatch, RecordBatchWriter},
};

pub fn new_arrow_writer<'a>(
    datas: &'a mut Vec<u8>,
    schema: &Schema,
) -> Result<StreamWriter<&'a mut Vec<u8>>, anyhow::Error> {
    Ok(StreamWriter::try_new(datas, schema)?)
}

pub fn write_recordbatch(record: &RecordBatch, schema: &Schema) -> Result<Vec<u8>, anyhow::Error> {
    let mut datas = Vec::new();
    let mut file = new_arrow_writer(&mut datas, schema)?;
    file.write(record)?;
    file.close()?;
    Ok(datas)
}
