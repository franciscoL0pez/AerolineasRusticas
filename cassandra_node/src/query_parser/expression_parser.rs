use super::custom_error::CustomError;
use super::expression::{Expression, Operand};
use super::tokenizer::Token;
use std::iter::Peekable;
use std::slice::Iter;

/// Parseauna expresión lógica dado un iterador de tokens, retornando un Expression que se estructura en forma de árbol.
/// El orden de precedencia de los operadores lógicos es el siguiente:
/// NOT, AND, OR
pub fn parse_expression(tokens: &mut Peekable<Iter<Token>>) -> Result<Expression, CustomError> {
    parse_or_expression(tokens) // primero entra en la de precedencia más baja
}

fn parse_or_expression(tokens: &mut Peekable<Iter<Token>>) -> Result<Expression, CustomError> {
    let mut expression = parse_and_expression(tokens)?; // entra en la de siguiente precedencia
    while let Some(Token::LogicalOperator(op)) = tokens.peek() {
        if op == "OR" {
            tokens.next();
            let right = parse_and_expression(tokens)?;
            expression = Expression::Or {
                // Se va armando el árbol de expresión
                left: Box::new(expression), // Esto es lo que se vino parseando con igual o mayor precedencia
                right: Box::new(right), // Esto es lo que se parsea después con mayor precedencia
            };
        } else {
            break;
        }
    }
    Ok(expression)
}

fn parse_and_expression(tokens: &mut Peekable<Iter<Token>>) -> Result<Expression, CustomError> {
    let mut expression = parse_not_expression(tokens)?; // entra en la de siguiente precedencia
    while let Some(Token::LogicalOperator(op)) = tokens.peek() {
        if op == "AND" {
            tokens.next();
            let right = parse_not_expression(tokens)?;
            expression = Expression::And {
                // Se va armando el árbol de expresión
                left: Box::new(expression), // Esto es lo que se vino parseando con igual o mayor precedencia
                right: Box::new(right), // Esto es lo que se parsea después con mayor precedencia
            };
        } else {
            break;
        }
    }
    Ok(expression)
}

fn parse_not_expression(tokens: &mut Peekable<Iter<Token>>) -> Result<Expression, CustomError> {
    if let Some(Token::LogicalOperator(op)) = tokens.peek() {
        if op == "NOT" {
            tokens.next();
            let expression = parse_primary_expression(tokens)?;
            return Ok(Expression::Not {
                // Se va armando el árbol de expresión
                right: Box::new(expression), // Esto es lo que se parsea después con mayor precedencia
            });
        }
    }
    parse_primary_expression(tokens)
}

fn parse_primary_expression(tokens: &mut Peekable<Iter<Token>>) -> Result<Expression, CustomError> {
    if let Some(Token::Symbol('(')) = tokens.peek() {
        // Si se abre paréntesis, se parsea la expresión que está adentro por completo
        tokens.next();
        let expression = parse_expression(tokens)?;
        if let Some(Token::Symbol(')')) = tokens.next() {
            // Verifica que haya un paréntesis de cierre
            return Ok(expression);
        } else {
            return Err(CustomError::InvalidSyntax {
                message: "Missing closing ')'".to_string(),
            });
        }
    }
    parse_comparison_expression(tokens) // Si no hay paréntesis, se parsea una expresión de comparación
}

fn parse_comparison_expression(
    tokens: &mut Peekable<Iter<Token>>,
) -> Result<Expression, CustomError> {
    if let Some(token) = tokens.peek() {
        match token {
            Token::Identifier(_) | Token::String(_) | Token::Integer(_) => {
                // Se parsea un operando
                let left = parse_operand(tokens)?;
                if let Some(Token::ComparisonOperator(op)) = tokens.next() {
                    // Verifica que haya un operador de comparación
                    let right = parse_operand(tokens)?; // Parsea el operando de la derecha
                    return Ok(Expression::Comparison {
                        left,
                        operator: op.to_string(),
                        right,
                    });
                }
            }
            _ => {
                return Err(CustomError::InvalidSyntax {
                    message: "Invalid expression".to_string(),
                })
            }
        }
    }
    Err(CustomError::InvalidSyntax {
        message: "Invalid expression".to_string(),
    })
}

fn parse_operand(tokens: &mut Peekable<Iter<Token>>) -> Result<Operand, CustomError> {
    if let Some(token) = tokens.next() {
        match token {
            Token::Identifier(string) => return Ok(Operand::Column(string.to_string())),
            Token::String(string) => return Ok(Operand::String(string.to_string())),
            Token::Integer(int) => return Ok(Operand::Integer(int.to_string())),
            other => {
                return Err(CustomError::InvalidSyntax {
                    message: format!("Invalid operand {:?}", other),
                })
            }
        }
    }
    Err(CustomError::InvalidSyntax {
        message: "No operand provided".to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_expression() {
        let tokens = [
            Token::Identifier("column1".to_string()),
            Token::ComparisonOperator("=".to_string()),
            Token::String("value1".to_string()),
        ];

        let result = parse_expression(&mut tokens.iter().peekable());

        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            Expression::Comparison {
                left: Operand::Column("column1".to_string()),
                operator: "=".to_string(),
                right: Operand::String("value1".to_string())
            }
        );
    }

    #[test]
    fn test_parse_expression_invalid_syntax() {
        let tokens = [Token::Identifier("column1".to_string())];

        let result = parse_expression(&mut tokens.iter().peekable());

        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap(),
            CustomError::InvalidSyntax {
                message: "Invalid expression".to_string()
            }
        );
    }

    #[test]
    fn test_parse_expression_missing_parenthesis() {
        let tokens = [
            Token::LogicalOperator("NOT".to_string()),
            Token::Symbol('('),
            Token::Identifier("column1".to_string()),
            Token::ComparisonOperator("=".to_string()),
            Token::String("value1".to_string()),
        ];

        let result = parse_expression(&mut tokens.iter().peekable());

        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap(),
            CustomError::InvalidSyntax {
                message: "Missing closing ')'".to_string()
            }
        );
    }

    #[test]
    fn test_parse_expression_invalid_operand() {
        let tokens = [
            Token::Identifier("column1".to_string()),
            Token::ComparisonOperator("=".to_string()),
            Token::LogicalOperator("AND".to_string()),
        ];

        let result = parse_expression(&mut tokens.iter().peekable());

        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap(),
            CustomError::InvalidSyntax {
                message: "Invalid operand LogicalOperator(\"AND\")".to_string()
            }
        );
    }
}
