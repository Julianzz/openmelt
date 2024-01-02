use anyhow::anyhow;
// use nom::{branch::alt, combinator::value, complete::tag};
use nom_locate::LocatedSpan;

use nom::{
    branch::alt,
    bytes::complete::{is_not, tag, tag_no_case},
    character::complete::{alpha1, alphanumeric1, char, multispace0, multispace1},
    combinator::{map, recognize},
    multi::{many0, many1},
    sequence::{delimited, pair, separated_pair, terminated, tuple},
};

use super::{ComparisionOperator, LogicOperator, QueryExpr};

pub type Span<'a> = LocatedSpan<&'a str>;
pub type IResult<'a, O> = nom::IResult<Span<'a>, O>;

pub fn parse_query(s: &str) -> Result<QueryExpr, anyhow::Error> {
    let (res, ops) = condition_expr(s.into()).unwrap();
    if !res.is_empty() {
        return Err(anyhow!("can not complete query : {}", res));
    }
    return Ok(ops);
}

fn comparision_operator(input: Span) -> IResult<ComparisionOperator> {
    alt((
        map(tag_no_case("!="), |_| ComparisionOperator::NotEqual),
        map(tag_no_case("<>"), |_| ComparisionOperator::NotEqual),
        map(tag_no_case("~="), |_| ComparisionOperator::Match),
        map(tag_no_case("=="), |_| ComparisionOperator::Equal),
        map(tag_no_case(">="), |_| ComparisionOperator::GreaterOrEqual),
        map(tag_no_case("<="), |_| ComparisionOperator::LessOrEqual),
        map(tag_no_case("="), |_| ComparisionOperator::Equal),
        map(tag_no_case(">"), |_| ComparisionOperator::Greater),
        map(tag_no_case("<"), |_| ComparisionOperator::Less),
    ))(input)
}

fn identifier(input: Span) -> IResult<&str> {
    // [a-zA-Z_][a-zA-Z0-9_]*
    let (rest, m) = recognize(pair(
        alt((alpha1, tag("_"))),
        many0(alt((alphanumeric1, tag("_"), tag(".")))),
    ))(input)?;
    Ok((rest, &m))
}

fn value_literal(input: Span) -> IResult<&str> {
    let (res, m) = recognize(many1(alphanumeric1))(input)?;
    Ok((res, &m))
}

fn string_literal(input: Span) -> IResult<&str> {
    let (rest, m) = recognize(delimited(char('"'), many0(is_not("\"")), char('"')))(input.into())?;
    Ok((rest, &m[1..m.len() - 1]))
}

fn compare_expr(input: Span) -> IResult<QueryExpr> {
    let (remaining, (left, _, ops, _, right)) = tuple((
        identifier,
        multispace0,
        comparision_operator,
        multispace0,
        alt((value_literal, string_literal)),
    ))(input)?;
    Ok((
        remaining,
        QueryExpr::ComparisonOp(left.to_string(), ops, right.to_string()),
    ))
}

fn condition_expr(input: Span) -> IResult<QueryExpr> {
    let cond = map(
        separated_pair(
            and_expr,
            delimited(multispace0, tag_no_case("or"), multispace1),
            condition_expr,
        ),
        |p| QueryExpr::LogicalOp(Box::new(p.0), LogicOperator::Or, Box::new(p.1)),
    );
    alt((cond, and_expr))(input)
}

fn and_expr(input: Span) -> IResult<QueryExpr> {
    let cond = map(
        separated_pair(
            parenthetical_expr,
            delimited(multispace0, tag_no_case("and"), multispace0),
            and_expr,
        ),
        |p| QueryExpr::LogicalOp(Box::new(p.0), LogicOperator::And, Box::new(p.1)),
    );

    alt((cond, parenthetical_expr))(input)
}

fn parenthetical_expr(i: Span) -> IResult<QueryExpr> {
    alt((
        compare_expr,
        map(
            delimited(
                terminated(tag("("), multispace0),
                condition_expr,
                delimited(multispace0, tag(")"), multispace0),
            ),
            |inner| inner,
        ),
    ))(i)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expr() {
        let expr = r#"(liuzhen==45 or time==zhong) and zhen==56 and cc=="zhong""#;
        let res = parse_query(expr).unwrap();
        let q1 = Box::new(QueryExpr::ComparisonOp(
            "liuzhen".into(),
            ComparisionOperator::Equal,
            "45".into(),
        ));
        let q2 = Box::new(QueryExpr::ComparisonOp(
            "time".into(),
            ComparisionOperator::Equal,
            "zhong".into(),
        ));
        let q3 = Box::new(QueryExpr::ComparisonOp(
            "zhen".into(),
            ComparisionOperator::Equal,
            "56".into(),
        ));
        let q4 = Box::new(QueryExpr::ComparisonOp(
            "cc".into(),
            ComparisionOperator::Equal,
            "zhong".into(),
        ));
        let expect = QueryExpr::LogicalOp(
            Box::new(QueryExpr::LogicalOp(
                Box::new(QueryExpr::LogicalOp(q1, LogicOperator::Or, q2)),
                LogicOperator::And,
                q3,
            )),
            LogicOperator::And,
            q4,
        );
        println!("{:?}  {:?}", res, expect);
    }
    #[test]
    fn test_parse() {
        let query = "kubernetes.docker_id==cbd7d9bb97cd64e6aac780711041d6a5d73861af6b5ed7842783df7037678113";
        let res = parse_query(query).unwrap();
        println!("{:?}", res);
    }
}
