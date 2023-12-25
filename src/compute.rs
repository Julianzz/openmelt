use datafusion::arrow::{
    array::AsArray,
    compute::{max_array, min_array},
    datatypes::ArrowNumericType,
    record_batch::RecordBatch,
};

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

// pub async fn compute_min_max_by_(
//     batch: RecordBatch,
//     name: &str,
// ) -> Result<(i64, i64), anyhow::Error> {
//     let ctx = SessionContext::new();
//     ctx.register_batch("t", batch)?;
//     let df = ctx.table("t").await?;
//     let df = df.aggregate(
//         vec![],
//         vec![min(col(name)).alias("min"), max(col(name)).alias("max")],
//     )?;
//     let res = df.collect().await?;
//     let batch = res.first().unwrap();

//     Ok((0, 0))
// }

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use datafusion::arrow::{array::{Int64Array, StringArray}, datatypes::Int64Type};

    use super::*;

    #[test]
    fn test_compute() {
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
}
