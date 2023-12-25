use actix_web::web::*;
use ahash::AHashMap;
use anyhow::*;

use chrono::prelude::*;
use datafusion::arrow::{datatypes::Int64Type, record_batch::RecordBatch};
use serde_derive::{Deserialize, Serialize};

use crate::{
    arrow::{self, recordbatch_to_jsons},
    compute,
    consts::*,
    exec::{self, Query},
    id_gen::gen_id,
    json_utils::{self, parse_timestamp},
    meta::{FileMeta, MetaService},
    parquet_utils, schema,
    schema::MeltSchema,
    storage::Storage,
};

use serde_json::Value;

#[derive(Clone, Serialize, Deserialize)]
pub struct Response {
    pub hits: Vec<Value>,
}

pub struct IngestService {
    meta: MetaService,
    storage: Storage,
}
impl IngestService {
    pub fn new(storage: Storage) -> IngestService {
        IngestService {
            meta: MetaService::new(),
            storage,
        }
    }

    pub fn ensure_dir(&self, table_name: &str, partition: &str) -> Result<String, anyhow::Error> {
        let path = format!("{table_name}/{partition}");
        std::fs::create_dir_all(&path)?;
        Ok(path)
    }

    pub fn process_bulk_item(&self, val: Value) -> Result<(i64, Value), anyhow::Error> {
        let stamp = parse_timestamp(TIMPSTAMP_FIELD_NAME, &val)?;
        let mut val = val;
        let local_val = val
            .as_object_mut()
            .ok_or(anyhow!("format wrong, can not convert to map"))?;
        local_val.insert(TIMPSTAMP_FIELD_NAME.into(), Value::Number(stamp.into()));
        Ok((stamp, val))
    }

    pub async fn bulk(&self, table_name: &str, body: Bytes) -> Result<(), anyhow::Error> {
        let records = json_utils::parse_lines(body)?;
        self.ingest_(table_name, records).await
    }

    pub async fn ingest(&self, table_name: &str, body: Bytes) -> Result<(), anyhow::Error> {
        let records = json_utils::parse_json(body)?;
        self.ingest_(table_name, records).await?;

        Ok(())
    }

    pub fn parquet_file_path(&self, partition: &str, name: &str) -> String {
        format!("{}/{}.{}", partition, name, PARQUET_EXT)
    }
    pub fn schema_file_path(&self, partition: &str, name: &str) -> String {
        format!("{}/{}.{}", partition, name, SCHEMA_EXT)
    }

    pub fn get_partition_key(&self, stamp: i64) -> String {
        let time_key = Utc.timestamp_nanos(stamp * 1000);
        time_key.format("%Y-%m-%d-%H").to_string()
    }

    fn partition_records(
        &self,
        records: Vec<Value>,
    ) -> Result<AHashMap<String, Vec<Value>>, anyhow::Error> {
        let mut partitions: AHashMap<String, Vec<Value>> = AHashMap::new();
        for record in records.into_iter() {
            let (stamp, val) = self.process_bulk_item(record)?;
            let partition_key = self.get_partition_key(stamp);
            let entry = partitions.entry(partition_key).or_default();
            entry.push(val);
        }
        Ok(partitions)
    }

    async fn ingest_(&self, table_name: &str, records: Vec<Value>) -> Result<(), anyhow::Error> {
        let partitions = self.partition_records(records)?;
        for (partition, records) in partitions {
            //infer schema
            let schema = schema::infer_schema(&records)?;
            self.write_partition(table_name, &partition, &schema, records)
                .await?;
        }
        Ok(())
    }

    async fn write_partition(
        &self,
        table_name: &str,
        partition: &str,
        schema: &MeltSchema,
        records: Vec<Value>,
    ) -> Result<(), anyhow::Error> {
        let batch = arrow::json_to_recordbatch(schema, &records)?;

        let partition_path = format!("{table_name}/{partition}");
        self.storage.ensure_dir(&partition_path)?;
        let segment_id = gen_id();
        let filename = self.parquet_file_path(&partition_path, &segment_id);
        let mut datas = Vec::new();
        let mut writer: parquet::arrow::ArrowWriter<&mut Vec<u8>> =
            parquet_utils::new_parquet_writer(&mut datas, schema)?;
        // arrow::write_file_arrow(filename, &records, schema.as_ref()).await?
        writer.write(&batch)?;
        writer.close()?;

        self.storage.put(&filename, datas.into()).await?;

        //write schema
        let filename = self.schema_file_path(&partition_path, &segment_id);
        self.storage
            .put(&filename, schema.serialize()?.into())
            .await?;
        let (min_ts, max_ts) = compute::compute_min_max::<Int64Type>(&batch, TIMPSTAMP_FIELD_NAME);

        self.meta.add_file(
            table_name,
            FileMeta::new(partition.into(), segment_id.into(), min_ts, max_ts),
        );
        Ok(())
    }

    pub async fn query(
        &self,
        table_name: &str,
        s: &str,
        min_ts: Option<i64>,
        max_ts: Option<i64>,
    ) -> Result<Response, anyhow::Error> {
        let batches = self.query_(table_name, s, min_ts, max_ts).await?;
        let batches = batches.iter().collect::<Vec<_>>();
        let batches = recordbatch_to_jsons(&batches)?;
        let batches: Vec<Value> = batches
            .into_iter()
            .filter(|v| !v.is_empty())
            .map(Value::Object)
            .collect();

        Ok(Response { hits: batches })
    }

    pub async fn query_(
        &self,
        table_name: &str,
        s: &str,
        min_ts: Option<i64>,
        max_ts: Option<i64>,
    ) -> Result<Vec<RecordBatch>, anyhow::Error> {
        let files = self.meta.query_files(table_name, min_ts, max_ts);
        let files = files
            .iter()
            .map(|f| {
                format!(
                    "{}/{}/{}/{}.{}",
                    self.storage.root(),
                    table_name,
                    f.partition(),
                    f.segment(),
                    PARQUET_EXT
                )
            })
            .collect::<Vec<_>>();

        let query = Query::from_str(s, min_ts, max_ts)?;
        // log::info!("query:{:?} file:{:?}", query, files);
        let res = exec::exec_search(&query, files).await?;
        Ok(res)
    }
}

#[cfg(test)]
mod tests {

    use crate::{arrow::recordbatch_to_jsons, storage::Storage, *};
    use actix_web::web::Bytes;

    use datafusion::arrow::datatypes::Int64Type;
    use serde_json::{json, Value};
    use tempfile::*;

    use super::IngestService;

    fn build_ingest_service() -> IngestService {
        let tmp_dir = tempdir().unwrap();
        let storage = Storage::new(tmp_dir);
        ingest::IngestService::new(storage)
    }
    #[tokio::test]
    async fn test_ingest_schema() {
        let data = r#"{"a": 1, "b": 1}"#;
        let name = "test";
        let data = Bytes::from(data);
        let s = build_ingest_service();

        match s.bulk(name, data).await {
            Ok(_) => println!("write success"),
            Err(err) => println!("{}", err.backtrace()),
        }
    }

    fn gen_test_data(prefix: &str) -> Vec<Value> {
        let mut datas = vec![];
        for i in 1..200 {
            let val = json!({
                "a" :i,
                "b": format!("{prefix}-{i}"),
            });
            datas.push(val);
        }
        datas
    }

    #[tokio::test]
    async fn test_ingest_service() {
        let datas = gen_test_data("f");
        let service = build_ingest_service();
        let table_name = "test";
        service.ingest_(table_name, datas).await.unwrap();

        let datas = gen_test_data("t");
        service.ingest_(table_name, datas).await.unwrap();

        let batches = service
            .query_(table_name, "a==1", None, None)
            .await
            .unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[1].num_rows(), 1);

        let batches = batches.iter().collect::<Vec<_>>();
        let json = recordbatch_to_jsons(&batches).unwrap();
        assert_eq!(json.len(), 2);
        assert_eq!(json[0]["a"].as_i64(), Some(1));
        assert_eq!(json[1]["a"].as_i64(), Some(1));
        assert_eq!(json[1]["b"].as_str(), Some("t-1"));
    }
    #[test]
    fn test_dataframe() {
        let data = r#"[{"a": 1, "b": 1},{"a": 2, "b": 1}]"#;
        // let name = "test";
        let data = Bytes::from(data);
        let s = build_ingest_service();
        let values = json_utils::parse_json(data).unwrap();
        let schema = schema::infer_schema(&values).unwrap();
        let batch = arrow::json_to_recordbatch(&schema, &values).unwrap();

        let (min, max) = compute::compute_min_max::<Int64Type>(&batch, "a");
        assert_eq!(min, 1);
        assert_eq!(max, 2);
    }
}
