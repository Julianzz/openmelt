use std::{iter, sync::Arc};

use arrow_schema::DataType;
use datafusion::arrow::array::{
    Array, ArrayBuilder, BooleanBuilder, Float16Builder, Float32Builder, Float64Builder,
    Int16Builder, Int32Builder, Int64Builder, Int8Builder, StringBuilder, UInt16Builder,
    UInt32Builder, UInt64Builder, UInt8Builder,
};

macro_rules! make_empty_array {
    ($builder:ty, $n:expr) => {{
        let mut builder = <$builder>::with_capacity($n);
        builder.append_nulls($n);
        ArrayBuilder::finish(&mut builder)
    }};
}

pub fn build_null_arrays(num: usize, t: &DataType) -> Result<Arc<dyn Array>, anyhow::Error> {
    match t {
        DataType::Boolean => Ok(make_empty_array!(BooleanBuilder, num)),
        DataType::Int8 => Ok(make_empty_array!(Int8Builder, num)),
        DataType::Int16 => Ok(make_empty_array!(Int16Builder, num)),
        DataType::Int32 => Ok(make_empty_array!(Int32Builder, num)),
        DataType::Int64 => Ok(make_empty_array!(Int64Builder, num)),
        DataType::UInt8 => Ok(make_empty_array!(UInt8Builder, num)),
        DataType::UInt16 => Ok(make_empty_array!(UInt16Builder, num)),
        DataType::UInt32 => Ok(make_empty_array!(UInt32Builder, num)),
        DataType::UInt64 => Ok(make_empty_array!(UInt64Builder, num)),
        DataType::Float16 => Ok(make_empty_array!(Float16Builder, num)),
        DataType::Float32 => Ok(make_empty_array!(Float32Builder, num)),
        DataType::Float64 => Ok(make_empty_array!(Float64Builder, num)),
        DataType::Utf8 => {
            let mut builder = StringBuilder::with_capacity(num, 0);
            iter::repeat(0).take(num).for_each(|_| {
                builder.append_null();
            });
            Ok(ArrayBuilder::finish(&mut builder))
        }
        _ => Err(anyhow::anyhow!("not support data type")),
    }
}

pub fn cast_array(array: Arc<dyn Array>, to: &DataType) -> Result<Arc<dyn Array>, anyhow::Error> {
    Ok(arrow_cast::cast(&array, to)?)
}

#[cfg(test)]
mod tests {
    use arrow_schema::{Field, Schema};

    use crate::fusion::recordbatch::build_tests_recordbatch;

    use super::*;

    #[test]
    fn test_cast() {
        let (schema, record) = build_tests_recordbatch();
        let to = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Utf8, false),
            Field::new("c", DataType::Int64, false),
        ]));
    }
}
