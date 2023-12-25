use anyhow::anyhow;
// use nom::{branch::alt, combinator::value, complete::tag};
use nom_locate::LocatedSpan;

use nom::{
    branch::alt,
    bytes::complete::{is_not, tag, tag_no_case},
    character::complete::{alpha1, alphanumeric1, char, multispace0, multispace1},
    combinator::{map, opt, recognize, value},
    multi::{many0, many1},
    sequence::{delimited, pair, separated_pair, terminated},
};

use crate::exec::QueryOps;

pub type Span<'a> = LocatedSpan<&'a str>;
pub type IResult<'a, O> = nom::IResult<Span<'a>, O>;

pub fn identifier(input: Span) -> IResult<&str> {
    // [a-zA-Z_][a-zA-Z0-9_]*
    let (rest, m) = recognize(pair(
        alt((alpha1, tag("_"))),
        many0(alt((alphanumeric1, tag("_"), tag(".")))),
    ))(input)?;
    Ok((rest, &m))
}

pub fn value_literal(input: Span) -> IResult<&str> {
    let (res, m) = recognize(many1(alphanumeric1))(input)?;
    Ok((res, &m))
}

pub fn string_literal(input: Span) -> IResult<&str> {
    let (rest, m) = recognize(delimited(char('"'), many0(is_not("\"")), char('"')))(input.into())?;
    Ok((rest, &m[1..m.len() - 1]))
}

pub fn compare_expr(input: Span) -> IResult<QueryOps> {
    alt((map(
        separated_pair(identifier, tag("=="), value_literal),
        |p| QueryOps::Equal(p.0.to_string(), p.1.to_string()),
    ),))(input)
}

pub fn condition_expr(input: Span) -> IResult<QueryOps> {
    let cond = map(
        separated_pair(
            and_expr,
            delimited(multispace0, tag_no_case("or"), multispace1),
            condition_expr,
        ),
        |p| QueryOps::Or(Box::new(p.0), Box::new(p.1)),
    );
    alt((cond, and_expr))(input)
}

pub fn and_expr(input: Span) -> IResult<QueryOps> {
    let cond = map(
        separated_pair(
            parenthetical_expr,
            delimited(multispace0, tag_no_case("and"), multispace0),
            and_expr,
        ),
        |p| QueryOps::And(Box::new(p.0), Box::new(p.1)),
    );

    alt((cond, parenthetical_expr))(input)
}

pub fn parenthetical_expr(i: Span) -> IResult<QueryOps> {
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

pub fn parse_query(s: &str) -> Result<QueryOps, anyhow::Error> {
    let (res, ops) = condition_expr(s.into()).unwrap();
    if !res.is_empty() {
        return Err(anyhow!("can not complete query : {}", res));
    }
    return Ok(ops);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expr() {
        let expr = "(liuzhen==45 or time==zhong) and zhen==56";
        let res = parse_query(expr).unwrap();
        let q1 = Box::new(QueryOps::Equal("liuzhen".into(), "45".into()));
        let q2 = Box::new(QueryOps::Equal("time".into(), "zhong".into()));
        let q3 = Box::new(QueryOps::Equal("zhen".into(), "56".into()));
        let expect = QueryOps::And(Box::new(QueryOps::Or(q1, q2)), q3);
        println!("{:?}  {:?}", res, expect);
    }
    #[test]
    fn test_parse() {
        let query = "kubernetes.docker_id==cbd7d9bb97cd64e6aac780711041d6a5d73861af6b5ed7842783df7037678113";
        let res = parse_query(query).unwrap();
        println!("{:?}", res);
    }
}
