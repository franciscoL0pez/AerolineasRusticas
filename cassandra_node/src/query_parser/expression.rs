use super::custom_error::CustomError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
/// Una expresión puede ser evaluada como verdadera o falsa.
pub enum Expression {
    True,
    And {
        left: Box<Expression>,
        right: Box<Expression>,
    },
    Or {
        left: Box<Expression>,
        right: Box<Expression>,
    },
    Not {
        right: Box<Expression>,
    },
    /// Los operadores soportados en esta implementación son:
    /// =, >, <, >=, <=
    Comparison {
        left: Operand,
        operator: String,
        right: Operand,
    },
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
/// Los operandos son la unidadad mínima de una expresión en esta implementación.
/// Pueden ser columnas, que consultan el valor de una columna en una fila, o valores literales limitados a Strings e Integers.
pub enum Operand {
    Column(String),
    String(String),
    Integer(String),
}

/// Evalúa una expresión dada un Hashmap de columnas y valores.
/// Retorna un booleano que indica si la expresión es verdadera o falsa.
pub fn evaluate_expression(
    expression: &Expression,
    row: &HashMap<String, String>,
) -> Result<bool, CustomError> {
    match expression {
        Expression::True => Ok(true),
        Expression::And { left, right } => {
            let left_result = evaluate_expression(left, row)?;
            let right_result = evaluate_expression(right, row)?;
            Ok(left_result && right_result)
        }
        Expression::Or { left, right } => {
            let left_result = evaluate_expression(left, row)?;
            let right_result = evaluate_expression(right, row)?;
            Ok(left_result || right_result)
        }
        Expression::Not { right } => {
            let right_result = evaluate_expression(right, row)?;
            Ok(!right_result)
        }
        Expression::Comparison {
            left,
            operator,
            right,
        } => {
            let left_value = evaluate_operand(left, row)?;
            let right_value = evaluate_operand(right, row)?;
            if let Ok(left_number) = str_to_number(&left_value) {
                if let Ok(right_number) = str_to_number(&right_value) {
                    return match operator.as_str() {
                        "=" => Ok(left_number == right_number),
                        ">" => Ok(left_number > right_number),
                        "<" => Ok(left_number < right_number),
                        ">=" => Ok(left_number >= right_number),
                        "<=" => Ok(left_number <= right_number),
                        _ => Err(CustomError::GenericError {
                            message: format!("Invalid operator: {}", operator),
                        }),
                    };
                }
            }
            match operator.as_str() {
                "=" => Ok(left_value == right_value),
                ">" => Ok(left_value > right_value),
                "<" => Ok(left_value < right_value),
                ">=" => Ok(left_value >= right_value),
                "<=" => Ok(left_value <= right_value),
                _ => Err(CustomError::GenericError {
                    message: format!("Invalid operator: {}", operator),
                }),
            }
        }
    }
}


/// Returns value given an expression "column = value" or "column = value AND ~".
///
/// #Parameters
/// - `expression`: Contains the expression with the comparison.
///
/// #Returns
/// - Value
///
pub fn extract_value_supposing_column_equals_value(expression: &Expression) -> Option<String> {
    match expression {
        Expression::Comparison {
            left: Operand::Column(_column_name),
            operator,
            right: Operand::String(value),
        } => {
            if operator == "=" {
                return Some(value.clone());
            }
        }
        Expression::Comparison {
            left: Operand::Column(_column_name),
            operator,
            right: Operand::Integer(value),
        } => {
            if operator == "=" {
                return Some(value.clone());
            }
        }
        Expression::And { left, .. } => match &**left {
            Expression::Comparison {
                left: Operand::Column(_column_name),
                operator,
                right: Operand::String(value),
            } => {
                if operator == "=" {
                    return Some(value.clone());
                }
            }
            Expression::Comparison {
                left: Operand::Column(_column_name),
                operator,
                right: Operand::Integer(value),
            } => {
                if operator == "=" {
                    return Some(value.clone());
                }
            }
            _ => {}
        },
        _ => {}
    }
    None
}

fn str_to_number(s: &str) -> Result<i32, CustomError> {
    if let Ok(number) = s.parse::<i32>() {
        Ok(number)
    } else {
        Err(CustomError::GenericError {
            message: format!("Invalid number: {}", s),
        })
    }
}

fn evaluate_operand(
    operand: &Operand,
    row: &HashMap<String, String>,
) -> Result<String, CustomError> {
    match operand {
        Operand::Column(column_name) => {
            if let Some(value) = row.get(column_name) {
                Ok(value.to_string())
            } else {
                Err(CustomError::GenericError {
                    message: format!("Column not found: {}", column_name),
                })
            }
        }
        Operand::String(value) | Operand::Integer(value) => Ok(value.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_evaluate_expression() {
        let mut row = HashMap::new();
        row.insert("column1".to_string(), "value1".to_string());
        row.insert("column2".to_string(), "value2".to_string());

        let expression = Expression::Comparison {
            left: Operand::Column("column1".to_string()),
            operator: "=".to_string(),
            right: Operand::String("value1".to_string()),
        };
        assert!(evaluate_expression(&expression, &row).unwrap());

        let expression = Expression::Comparison {
            left: Operand::Column("column1".to_string()),
            operator: "=".to_string(),
            right: Operand::String("value2".to_string()),
        };
        assert!(!evaluate_expression(&expression, &row).unwrap());

        let expression = Expression::Comparison {
            left: Operand::Column("column1".to_string()),
            operator: ">".to_string(),
            right: Operand::String("value2".to_string()),
        };
        assert!(!evaluate_expression(&expression, &row).unwrap());

        let expression = Expression::Comparison {
            left: Operand::Column("column1".to_string()),
            operator: ">=".to_string(),
            right: Operand::String("value2".to_string()),
        };
        assert!(!evaluate_expression(&expression, &row).unwrap());

        let expression = Expression::Comparison {
            left: Operand::Column("column1".to_string()),
            operator: ">=".to_string(),
            right: Operand::String("value1".to_string()),
        };
        assert!(evaluate_expression(&expression, &row).unwrap());

        let expression = Expression::Comparison {
            left: Operand::Column("column1".to_string()),
            operator: "<".to_string(),
            right: Operand::String("value2".to_string()),
        };
        assert!(evaluate_expression(&expression, &row).unwrap());
    }
}
