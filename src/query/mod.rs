use serde_derive::{Deserialize, Serialize};

mod parser;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ComparisionOperator {
    Equal,
    NotEqual,
    GreaterOrEqual,
    LessOrEqual,
    Less,
    Greater,
    Match,
}

#[derive(Debug)]
pub enum LogicOperator {
    Or,
    And,
}

#[derive(Debug)]
pub enum QueryExpr {
    ComparisonOp(String, ComparisionOperator, String),
    LogicalOp(Box<QueryExpr>, LogicOperator, Box<QueryExpr>),
}

impl QueryExpr {
    pub fn from_str(s: &str) -> Result<QueryExpr, anyhow::Error> {
        parser::parse_query(s)
    }
}
