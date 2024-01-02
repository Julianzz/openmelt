use arrow_schema::Schema;
use datafusion::{
    arrow::record_batch::RecordBatch,
    execution::{context::SessionContext, options::ParquetReadOptions},
    logical_expr::{and, ident, lit, or, Expr},
};

use crate::{
    config::TIMPSTAMP_FIELD_NAME,
    query::{ComparisionOperator, LogicOperator, QueryExpr},
};

pub struct Query {
    expr: QueryExpr,
    min_ts: Option<i64>,
    max_ts: Option<i64>,
}

impl Query {
    pub fn from_str(
        s: &str,
        min_ts: Option<i64>,
        max_ts: Option<i64>,
    ) -> Result<Query, anyhow::Error> {
        let ops = QueryExpr::from_str(s)?;
        Ok(Query {
            expr: ops,
            min_ts,
            max_ts,
        })
    }

    pub fn to_exp(&self, schema: &Schema) -> Expr {
        let expr = queryexpr_to_expr(&self.expr, schema);
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

pub fn queryexpr_to_expr(ops: &QueryExpr, schema: &Schema) -> Expr {
    match ops {
        QueryExpr::LogicalOp(left, ops, right) => match ops {
            LogicOperator::Or => and(
                queryexpr_to_expr(left, schema),
                queryexpr_to_expr(right, schema),
            ),
            LogicOperator::And => or(
                queryexpr_to_expr(left, schema),
                queryexpr_to_expr(right, schema),
            ),
        },

        QueryExpr::ComparisonOp(name, ops, value) => match ops {
            ComparisionOperator::Equal => ident(name).eq(lit(value)),
            ComparisionOperator::NotEqual => ident(name).not_eq(lit(value)),
            ComparisionOperator::GreaterOrEqual => ident(name).gt_eq(lit(value)),
            ComparisionOperator::LessOrEqual => ident(name).lt_eq(lit(value)),
            ComparisionOperator::Less => ident(name).lt(lit(value)),
            ComparisionOperator::Greater => ident(name).gt(lit(value)),
            ComparisionOperator::Match => ident(name).like(lit(value)),
        },
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

    use crate::{
        exec::queryexpr_to_expr,
        fusion::recordbatch::{build_tests_recordbatch, recordbatch_to_jsons},
        query::{ComparisionOperator, LogicOperator, QueryExpr},
    };

    #[tokio::test]
    async fn test_name() {
        let (schema, batch) = build_tests_recordbatch();

        let left = QueryExpr::ComparisonOp("b".into(), ComparisionOperator::Equal, "a".into());
        let right = QueryExpr::ComparisonOp("a".into(), ComparisionOperator::Equal, "a".into());
        let ops = QueryExpr::LogicalOp(Box::new(left), LogicOperator::Or, Box::new(right));
        let expr = queryexpr_to_expr(&ops, &schema);

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
