use serde_json::Value;
use crate::core::types::{DbError, Result};

#[derive(Debug, Clone, PartialEq)]
pub enum Operator {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    Contains,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    String(String),
    Number(f64),
    Bool(bool),
    Null,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Condition {
    pub path: String,
    pub op: Operator,
    pub value: Literal,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    Single(Condition),
    And(Vec<Expression>),
    Or(Vec<Expression>),
}

#[derive(Debug, Clone)]
pub struct QueryFilter {
    expr: Expression,
}

impl QueryFilter {
    pub fn parse(input: &str) -> Result<Self> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Err(DbError::InvalidCommand("Empty filter query".into()));
        }

        // Support OR expressions separated by " OR "
        let or_parts: Vec<&str> = trimmed.split(" OR ").collect();
        if or_parts.len() > 1 {
            let mut exprs = Vec::new();
            for part in or_parts {
                exprs.push(Self::parse_and_expr(part)?);
            }
            return Ok(Self {
                expr: Expression::Or(exprs),
            });
        }

        let expr = Self::parse_and_expr(trimmed)?;
        Ok(Self { expr })
    }

    fn parse_and_expr(input: &str) -> Result<Expression> {
        let and_parts: Vec<&str> = input.split(" AND ").collect();
        if and_parts.len() > 1 {
            let mut exprs = Vec::new();
            for part in and_parts {
                exprs.push(Expression::Single(Self::parse_single_condition(part)?));
            }
            Ok(Expression::And(exprs))
        } else {
            Ok(Expression::Single(Self::parse_single_condition(input)?))
        }
    }

    fn parse_single_condition(input: &str) -> Result<Condition> {
        let trimmed = input.trim();

        // Operators to match in order of specificity
        let ops = [
            (">=", Operator::Gte),
            ("<=", Operator::Lte),
            ("!=", Operator::Ne),
            ("==", Operator::Eq),
            ("=", Operator::Eq),
            (">", Operator::Gt),
            ("<", Operator::Lt),
            (" CONTAINS ", Operator::Contains),
            (" contains ", Operator::Contains),
        ];

        for (op_str, op) in ops {
            if let Some(pos) = trimmed.find(op_str) {
                let path = trimmed[..pos].trim().to_string();
                let val_str = trimmed[pos + op_str.len()..].trim();

                if path.is_empty() || val_str.is_empty() {
                    return Err(DbError::InvalidCommand(format!(
                        "Invalid condition syntax: '{}'",
                        input
                    )));
                }

                let value = Self::parse_literal(val_str);
                return Ok(Condition { path, op, value });
            }
        }

        Err(DbError::InvalidCommand(format!(
            "Could not parse condition: '{}'",
            input
        )))
    }

    fn parse_literal(s: &str) -> Literal {
        let trimmed = s.trim();
        // Quoted string
        if (trimmed.starts_with('"') && trimmed.ends_with('"'))
            || (trimmed.starts_with('\'') && trimmed.ends_with('\''))
        {
            if trimmed.len() >= 2 {
                return Literal::String(trimmed[1..trimmed.len() - 1].to_string());
            }
        }

        // Boolean
        if trimmed.eq_ignore_ascii_case("true") {
            return Literal::Bool(true);
        }
        if trimmed.eq_ignore_ascii_case("false") {
            return Literal::Bool(false);
        }
        if trimmed.eq_ignore_ascii_case("null") {
            return Literal::Null;
        }

        // Number
        if let Ok(num) = trimmed.parse::<f64>() {
            return Literal::Number(num);
        }

        // Unquoted string fallback
        Literal::String(trimmed.to_string())
    }

    /// Evaluates if a JSON slice matches this filter
    pub fn matches(&self, json_bytes: &[u8]) -> bool {
        if let Ok(parsed_json) = serde_json::from_slice::<Value>(json_bytes) {
            self.eval_expression(&self.expr, &parsed_json)
        } else {
            false
        }
    }

    /// Evaluates if a parsed JSON Value matches this filter
    pub fn matches_value(&self, value: &Value) -> bool {
        self.eval_expression(&self.expr, value)
    }

    fn eval_expression(&self, expr: &Expression, root: &Value) -> bool {
        match expr {
            Expression::Single(cond) => self.eval_condition(cond, root),
            Expression::And(list) => list.iter().all(|e| self.eval_expression(e, root)),
            Expression::Or(list) => list.iter().any(|e| self.eval_expression(e, root)),
        }
    }

    fn eval_condition(&self, cond: &Condition, root: &Value) -> bool {
        let target_val = Self::get_json_path(root, &cond.path);

        match (&cond.op, target_val) {
            (Operator::Eq, Some(val)) => match (&cond.value, val) {
                (Literal::String(s), Value::String(vs)) => s == vs,
                (Literal::Number(n), Value::Number(vn)) => vn.as_f64().map_or(false, |f| (f - n).abs() < 1e-9),
                (Literal::Bool(b), Value::Bool(vb)) => b == vb,
                (Literal::Null, Value::Null) => true,
                _ => false,
            },
            (Operator::Ne, Some(val)) => match (&cond.value, val) {
                (Literal::String(s), Value::String(vs)) => s != vs,
                (Literal::Number(n), Value::Number(vn)) => vn.as_f64().map_or(true, |f| (f - n).abs() >= 1e-9),
                (Literal::Bool(b), Value::Bool(vb)) => b != vb,
                (Literal::Null, Value::Null) => false,
                _ => true,
            },
            (Operator::Ne, None) => true, // Field does not exist, so != condition holds
            (Operator::Gt, Some(val)) => match (&cond.value, val) {
                (Literal::Number(n), Value::Number(vn)) => vn.as_f64().map_or(false, |f| f > *n),
                _ => false,
            },
            (Operator::Gte, Some(val)) => match (&cond.value, val) {
                (Literal::Number(n), Value::Number(vn)) => vn.as_f64().map_or(false, |f| f >= *n),
                _ => false,
            },
            (Operator::Lt, Some(val)) => match (&cond.value, val) {
                (Literal::Number(n), Value::Number(vn)) => vn.as_f64().map_or(false, |f| f < *n),
                _ => false,
            },
            (Operator::Lte, Some(val)) => match (&cond.value, val) {
                (Literal::Number(n), Value::Number(vn)) => vn.as_f64().map_or(false, |f| f <= *n),
                _ => false,
            },
            (Operator::Contains, Some(val)) => match (&cond.value, val) {
                (Literal::String(s), Value::String(vs)) => vs.contains(s),
                (Literal::String(s), Value::Array(arr)) => arr.iter().any(|item| {
                    item.as_str().map_or(false, |item_str| item_str == s)
                }),
                _ => false,
            },
            _ => false,
        }
    }

    pub fn get_json_path<'a>(root: &'a Value, path: &str) -> Option<&'a Value> {
        let mut current = root;
        for part in path.split('.') {
            match current {
                Value::Object(map) => {
                    current = map.get(part)?;
                }
                _ => return None,
            }
        }
        Some(current)
    }

    /// Sets or updates a value at a dotted path, creating intermediate objects as needed
    pub fn set_json_path(root: &mut Value, path: &str, new_val: Value) {
        let parts: Vec<&str> = path.split('.').collect();
        if parts.is_empty() {
            return;
        }

        if !root.is_object() {
            *root = Value::Object(serde_json::Map::new());
        }

        let mut current = root;
        for (i, part) in parts.iter().enumerate() {
            if i == parts.len() - 1 {
                if let Value::Object(map) = current {
                    map.insert((*part).to_string(), new_val);
                }
                return;
            }

            if let Value::Object(map) = current {
                if !map.contains_key(*part) || !map[*part].is_object() {
                    map.insert((*part).to_string(), Value::Object(serde_json::Map::new()));
                }
                current = map.get_mut(*part).unwrap();
            } else {
                return;
            }
        }
    }

    /// Extract numeric value for aggregation (COUNT, SUM, AVG, STATS)
    pub fn extract_number(root: &Value, path: &str) -> Option<f64> {
        Self::get_json_path(root, path).and_then(|v| match v {
            Value::Number(n) => n.as_f64(),
            Value::String(s) => s.parse::<f64>().ok(),
            _ => None,
        })
    }

    /// Parses string into strongly typed serde_json::Value (JSON, Number, Boolean, String)
    pub fn parse_json_value(raw: &str) -> Value {
        let trimmed = raw.trim();
        if trimmed.starts_with('\'') && trimmed.ends_with('\'') && trimmed.len() >= 2 {
            return Value::String(trimmed[1..trimmed.len() - 1].to_string());
        }
        if let Ok(v) = serde_json::from_str::<Value>(trimmed) {
            v
        } else if let Ok(n) = trimmed.parse::<i64>() {
            Value::Number(n.into())
        } else if let Ok(f) = trimmed.parse::<f64>() {
            serde_json::Number::from_f64(f)
                .map(Value::Number)
                .unwrap_or_else(|| Value::String(trimmed.to_string()))
        } else if trimmed.eq_ignore_ascii_case("true") {
            Value::Bool(true)
        } else if trimmed.eq_ignore_ascii_case("false") {
            Value::Bool(false)
        } else if trimmed.eq_ignore_ascii_case("null") {
            Value::Null
        } else {
            Value::String(trimmed.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_filter_simple() {
        let json = br#"{"name":"Alex","level":42,"active":true,"tags":["pro","vip"]}"#;

        let filter = QueryFilter::parse("level >= 40").unwrap();
        assert!(filter.matches(json));

        let filter = QueryFilter::parse("level < 40").unwrap();
        assert!(!filter.matches(json));

        let filter = QueryFilter::parse("name == \"Alex\"").unwrap();
        assert!(filter.matches(json));

        let filter = QueryFilter::parse("active == true").unwrap();
        assert!(filter.matches(json));

        let filter = QueryFilter::parse("tags CONTAINS \"vip\"").unwrap();
        assert!(filter.matches(json));
    }

    #[test]
    fn test_query_filter_compound() {
        let json = br#"{"stats":{"kills":150,"deaths":10},"status":"online"}"#;

        let filter = QueryFilter::parse("stats.kills > 100 AND stats.deaths <= 20").unwrap();
        assert!(filter.matches(json));

        let filter = QueryFilter::parse("status == 'offline' OR stats.kills > 100").unwrap();
        assert!(filter.matches(json));
    }
}
