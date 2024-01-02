use arrow_schema::Schema;
use datafusion::{
    arrow::{
        array::{Array, AsArray},
        compute::{concat_batches, max_array, min_array},
        datatypes::ArrowNumericType,
        record_batch::RecordBatch,
    },
    datasource::MemTable,
    execution::context::SessionContext,
    logical_expr::ident,
    sql::TableReference,
};
use std::sync::Arc;

use super::array;

pub fn compute_min_max<T: ArrowNumericType>(
    record: &RecordBatch,
    name: &str,
) -> (T::Native, T::Native) {
    let array = record.column_by_name(name).unwrap();
    let b = array.as_primitive::<T>();
    let max = max_array::<T, _>(b).unwrap();
    let min = min_array::<T, _>(b).unwrap();
    (min, max)
}

pub async fn sort(
    batch: RecordBatch,
    field: &[(&str, bool)],
) -> Result<RecordBatch, anyhow::Error> {
    Ok(sort_batches(batch.schema(), vec![batch], field).await?)
}

pub async fn sort_batches(
    schema: Arc<Schema>,
    records: Vec<RecordBatch>,
    sort_by: &[(&str, bool)],
) -> Result<RecordBatch, anyhow::Error> {
    let ctx = SessionContext::new();
    let table = MemTable::try_new(schema.clone(), vec![records])?;
    ctx.register_table(TableReference::Bare { table: "t".into() }, Arc::new(table))?;

    let df = ctx.table("t").await?;
    let expr: Vec<_> = sort_by
        .into_iter()
        .map(|(f, asc)| ident(*f).sort(*asc, false))
        .collect();
    let df = df.sort(expr)?;
    let batches = df.collect().await?;
    let batch = concat_batches(&schema, &batches)?;
    Ok(batch)
}

pub fn cast(schema: &Arc<Schema>, batch: RecordBatch) -> Result<RecordBatch, anyhow::Error> {
    let row_num = batch.num_rows();
    let arrays = schema
        .all_fields()
        .iter()
        .map(move |f| {
            let field = batch.column_by_name(f.name());
            if let Some(array) = field {
                if array.data_type() == f.data_type() {
                    Ok(array.clone())
                } else {
                    array::cast_array(array.clone(), f.data_type())
                }
            } else {
                array::build_null_arrays(row_num, f.data_type())
            }
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(RecordBatch::try_new(schema.clone(), arrays)?)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::fusion::recordbatch::build_tests_recordbatch;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::arrow::{
        array::{Int64Array, StringArray},
        datatypes::Int64Type,
    };

    #[test]
    fn test_compute_min_max() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Int64, false),
        ]));

        // define data.
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
                Arc::new(Int64Array::from(vec![1, 10, 10, 100])),
            ],
        )
        .unwrap();

        let (a, b) = compute_min_max::<Int64Type>(&batch, "b");
        assert_eq!(a, 1);
        assert_eq!(b, 100);
    }

    #[tokio::test]
    async fn test_sort_batch() {
        let (schema, record) = build_tests_recordbatch();
        let record = sort(record, &vec![("a", false)]).await.unwrap();
        let a: &StringArray = record
            .column_by_name("a")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();

        let b = StringArray::from(vec!["d", "c", "b", "a"]);
        for i in 0..b.len() {
            assert_eq!(a.value(i), b.value(i));
        }
        println!("{:?}", &record);
    }

    #[test]
    fn test_cast() {
        let (schema, record) = build_tests_recordbatch();
        let to = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Int64, true),
        ]));

        let res = cast(&to, record).unwrap();
        assert_eq!(res.num_columns(), to.fields().len());
        assert_eq!(res.schema(), to);
        // assert_eq!(res.slice(offset, length));
        let b = StringArray::from(vec!["1", "10", "10", "100"]);
        let a: &StringArray = res
            .column_by_name("b")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        for i in 0..b.len() {
            assert_eq!(a.value(i), b.value(i));
        }
        let c: &Int64Array = res
            .column_by_name("c")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        for i in 0..c.len() {
            assert_eq!(c.is_null(i), true);
        }
    }
}
