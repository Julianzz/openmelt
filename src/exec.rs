use crate::{consts::*, parser};
use arrow_schema::Schema;
use datafusion::{
    arrow::record_batch::RecordBatch,
    execution::{context::SessionContext, options::ParquetReadOptions},
    logical_expr::{and, lit, or, Expr, ident},
};

#[derive(Debug)]
pub struct Query {
    ops: QueryOps,
    min_ts: Option<i64>,
    max_ts: Option<i64>,
}

impl Query {
    pub fn from_str(
        s: &str,
        min_ts: Option<i64>,
        max_ts: Option<i64>,
    ) -> Result<Query, anyhow::Error> {
        let ops = QueryOps::from_str(s)?;
        Ok(Query {
            ops,
            min_ts,
            max_ts,
        })
    }

    pub fn to_exp(&self, schema: &Schema) -> Expr {
        let expr = self.ops.to_exp(schema);
        let expr = if let Some(t) = self.min_ts {
            expr.and(ident(TIMPSTAMP_FIELD_NAME).gt_eq(lit(t)))
        } else {
            expr
        };
        let expr = if let Some(t) = self.max_ts {
            expr.and(ident(TIMPSTAMP_FIELD_NAME).lt_eq(lit(t)))
        } else {
            expr
        };
        expr
    }
}

#[derive(Debug)]
pub enum QueryOps {
    And(Box<QueryOps>, Box<QueryOps>),
    Or(Box<QueryOps>, Box<QueryOps>),
    Equal(String, String),
}

impl QueryOps {
    pub fn from_str(s: &str) -> Result<QueryOps, anyhow::Error> {
        parser::parse_query(s)
    }

    pub fn to_exp(&self, schema: &Schema) -> Expr {
        match self {
            QueryOps::And(left, right) => and(left.to_exp(schema), right.to_exp(schema)),
            QueryOps::Or(left, right) => or(left.to_exp(schema), right.to_exp(schema)),
            QueryOps::Equal(name, value) => ident(name).eq(lit(value)),
        }
    }
}

//TODO
pub async fn exec_search(
    query: &Query,
    files: Vec<String>,
) -> Result<Vec<RecordBatch>, anyhow::Error> {
    let mut res = vec![];
    let ctx = SessionContext::new();
    for file in files {
        ctx.register_parquet("t", &file, ParquetReadOptions::default())
            .await?;
        let expr = query.to_exp(&Schema::empty());
        let df = ctx.table("t").await.unwrap();
        let df = df.filter(expr).unwrap();
        let records = df.collect().await.unwrap();
        ctx.deregister_table("t")?;
        res.extend_from_slice(&records);
    }

    Ok(res)
}

#[cfg(test)]
mod tests {
    use datafusion::execution::context::SessionContext;

    use crate::{arrow::recordbatch_to_jsons, tests_utils::build_tests_recordbatch};

    use super::*;

    #[tokio::test]
    async fn test_name() {
        let (schema, batch) = build_tests_recordbatch();

        let left = QueryOps::Equal("b".into(), "a".into());
        let right = QueryOps::Equal("a".into(), "a".into());
        let ops = QueryOps::Or(Box::new(left), Box::new(right));
        let expr = ops.to_exp(&schema);

        let ctx = SessionContext::new();
        ctx.register_batch("t", batch).unwrap();
        let df = ctx.table("t").await.unwrap();
        let df = df.filter(expr).unwrap();
        let res = df.collect().await.unwrap();
        // let batch = res.first();
        let res = res.iter().collect::<Vec<_>>();
        let res = recordbatch_to_jsons(&res).unwrap();
        let res = serde_json::to_string(&res).unwrap();
        println!("{}", res);
    }
}
