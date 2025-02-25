use std::collections::HashMap;
use std::iter::Peekable;
use std::slice::Iter;
mod custom_error;
use custom_error::CustomError;
mod tokenizer;
use tokenizer::{tokenize, Token};
pub mod expression;
use expression::Expression;
mod expression_parser;
use expression_parser::parse_expression;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
/// Enum representing the different types of messages that can be sent between nodes
pub enum ParsedQuery {
    CreateKeyspace {
        keyspace_name: String,
        replication_strategy: String,
        replication_factor: String,
    },
    CreateTable {
        table_name: String,
        columns: Vec<(String, String)>,
        partition_key_columns: Vec<String>,
        clustering_key_columns: Vec<String>,
    },
    Insert {
        table_name: String,
        columns_in_order: Vec<String>,
        rows_to_insert: Vec<HashMap<String, String>>,
    },
    Update {
        table_name: String,
        values_to_update: HashMap<String, String>,
        condition: Expression,
    },
    Delete {
        table_name: String,
        condition: Expression,
    },
    Select {
        table_name: String,
        columns: Vec<String>,
        condition: Expression,
        order_by: Vec<(String, String)>,
    },
    UseKeyspace {
        keyspace_name: String,
    },
}

impl ParsedQuery {
    /// Returns the table name of the query
    pub fn get_table_name(&self) -> Result<String, String> {
        match self {
            Self::CreateTable { table_name, .. } => Ok(table_name.to_string()),
            Self::Insert { table_name, .. } => Ok(table_name.to_string()),
            Self::Update { table_name, .. } => Ok(table_name.to_string()),
            Self::Delete { table_name, .. } => Ok(table_name.to_string()),
            Self::Select { table_name, .. } => Ok(table_name.to_string()),
            _ => Err("No table name found".to_string()),
        }
    }

    /// Returns the keyspace name of the query
    pub fn get_keyspace_name(&self) -> Result<String, String> {
        match self {
            Self::CreateKeyspace { keyspace_name, .. } => Ok(keyspace_name.to_string()),
            _ => Err("No keyspace name found".to_string()),
        }
    }

    /// Returns the replication strategy of the query
    pub fn get_replication_strategy(&self) -> Result<String, String> {
        match self {
            Self::CreateKeyspace {
                replication_strategy,
                ..
            } => Ok(replication_strategy.to_string()),
            _ => Err("No replication strategy found".to_string()),
        }
    }

    /// Returns the replication factor of the query
    pub fn get_replication_factor(&self) -> Result<String, String> {
        match self {
            Self::CreateKeyspace {
                replication_factor, ..
            } => Ok(replication_factor.to_string()),
            _ => Err("No replication factor found".to_string()),
        }
    }

    /// Returns the columns of the query
    pub fn get_columns_with_type(&self) -> Result<Vec<(String, String)>, String> {
        match self {
            Self::CreateTable { columns, .. } => Ok(columns.clone()),
            _ => Err("No columns found".to_string()),
        }
    }

    /// Returns the columns of the query
    pub fn get_columns(&self) -> Result<Vec<String>, String> {
        match self {
            Self::Select { columns, .. } => Ok(columns.clone()),
            _ => Err("No columns found".to_string()),
        }
    }

    /// Returns the partition key columns of the query
    pub fn get_partition_key_columns(&self) -> Result<Vec<String>, String> {
        match self {
            Self::CreateTable {
                partition_key_columns,
                ..
            } => Ok(partition_key_columns.clone()),
            _ => Err("No partition key columns found".to_string()),
        }
    }

    /// Returns the clustering key columns of the query
    pub fn get_clustering_key_columns(&self) -> Result<Vec<String>, String> {
        match self {
            Self::CreateTable {
                clustering_key_columns,
                ..
            } => Ok(clustering_key_columns.clone()),
            _ => Err("No clustering key columns found".to_string()),
        }
    }

    /// Returns the values to update of the query
    pub fn get_values_to_update(&self) -> Result<HashMap<String, String>, String> {
        match self {
            Self::Update {
                values_to_update, ..
            } => Ok(values_to_update.clone()),
            _ => Err("No values to update found".to_string()),
        }
    }

    /// Returns the condition of the query
    pub fn get_condition(&self) -> Result<Expression, String> {
        match self {
            Self::Update { condition, .. } => Ok(condition.clone()),
            Self::Delete { condition, .. } => Ok(condition.clone()),
            Self::Select { condition, .. } => Ok(condition.clone()),
            _ => Err("No condition found".to_string()),
        }
    }

    /// Returns the order by of the query
    pub fn get_order_by(&self) -> Result<Vec<(String, String)>, String> {
        match self {
            Self::Select { order_by, .. } => Ok(order_by.clone()),
            _ => Err("No order by found".to_string()),
        }
    }

    /// Returns the rows to insert of the query
    pub fn get_rows_to_insert(&self) -> Result<Vec<HashMap<String, String>>, String> {
        match self {
            Self::Insert {
                rows_to_insert: values_row,
                ..
            } => Ok(values_row.clone()),
            _ => Err("No values row found".to_string()),
        }
    }
}

// Given a string, returns a vector of exploded instructions
pub fn parse_instruction(query_string: &str) -> Result<ParsedQuery, CustomError> {
    let tokens = tokenize(query_string)?;
    if let Some(Token::Keyword(keyword)) = tokens.first() {
        match keyword.as_str() {
            "CREATE" => {
                let res = parse_create(&tokens);
                // dbg!(&res);
                return res;
            }
            "INSERT" => {
                let res = parse_insert(&tokens);
                // dbg!(&res);
                return res;
            }
            "UPDATE" => return parse_update(&tokens),
            "DELETE" => return parse_delete(&tokens),
            "SELECT" => {
                let res = parse_select(&tokens);
                // dbg!(&res);
                return res;
            }
            "USE" => return parse_use(&tokens),
            other => {
                CustomError::error_invalid_syntax(&format!("Invalid command: {}", other))?;
            }
        }
    } else {
        CustomError::error_invalid_syntax("Usage: <COMMAND> <...>")?;
    }
    Err(CustomError::InvalidSyntax {
        message: "Error parsing instruction".to_string(),
    })
}

// Functions used to parse USE

fn parse_use(tokens: &[Token]) -> Result<ParsedQuery, CustomError> {
    if tokens.len() != 3 {
        CustomError::error_invalid_syntax("Usage: USE <keyspace_name>;")?;
    }
    let keyspace_name = match tokens.get(1) {
        Some(Token::Identifier(name)) | Some(Token::String(name)) => name.to_string(),
        _ => {
            CustomError::error_invalid_syntax("Expected keyspace name after USE")?;
            "".to_string()
        }
    };
    if let Some(Token::Symbol(';')) = tokens.get(2) {
    } else {
        CustomError::error_invalid_syntax("Expected ';' after keyspace name")?;
    }
    Ok(ParsedQuery::UseKeyspace {
        keyspace_name: keyspace_name.clone(),
    })
}

// Functions used to parse INSERT

fn parse_insert(tokens: &[Token]) -> Result<ParsedQuery, CustomError> {
    let (table_name, columns_in_order, rows) = parse_insert_variables(tokens)?;
    Ok(ParsedQuery::Insert {
        table_name: table_name.clone(),
        columns_in_order,
        rows_to_insert: rows,
    })
}
type QueryResult = Result<(String, Vec<String>, Vec<HashMap<String, String>>), CustomError>;

fn parse_insert_variables(
    tokens: &[Token],
) -> QueryResult {
    let mut iter = tokens.iter().peekable();
    iter.next(); // salteo el INSERT
    let table_name = parse_insert_into(&mut iter)?;
    let columns = parse_insert_columns(&mut iter)?;
    let rows = parse_insert_values(&mut iter, &columns)?;
    check_ending_with_semicolon(&mut iter)?;
    Ok((table_name, columns, rows))
}

fn parse_insert_into(iter: &mut Peekable<Iter<Token>>) -> Result<String, CustomError> {
    if !matches!(iter.next(), Some(Token::Keyword(keyword)) if keyword.as_str() == "INTO") {
        // Verifico que haya INTO
        CustomError::error_invalid_syntax("Expected INTO after INSERT")?;
    }
    if let Some(Token::Identifier(name)) | Some(Token::String(name)) = iter.next() {
        return Ok(name.to_string());
    } else {
        CustomError::error_invalid_syntax("Expected table name after INTO")?;
    }
    Ok("".to_string())
}

fn parse_insert_columns(iter: &mut Peekable<Iter<Token>>) -> Result<Vec<String>, CustomError> {
    let mut columns = vec![];
    if let Some(Token::Symbol('(')) = iter.next() {
        // Verifico que se abra parentesis
        while let Some(token) = iter.next() {
            // Este ciclo termina al encontrar un ')'
            match token {
                Token::Identifier(name) | Token::String(name) => {
                    // Si es nombre de columna, lo agrego
                    columns.push(name.to_string());
                    if let Some(Token::Symbol(')')) | Some(Token::Symbol(',')) = iter.peek() {
                    } else {
                        CustomError::error_invalid_syntax("Expected ',' or ')' after column name")?;
                    }
                }
                Token::Symbol(',') => {
                    // Si es coma, verifico que su siguiente sea nombre de columna
                    if let Some(Token::Identifier(_)) | Some(Token::String(_)) = iter.peek() {
                    } else {
                        CustomError::error_invalid_syntax("Expected column name after ','")?;
                    }
                }
                Token::Symbol(')') => {
                    // Si se cierra parentesis, termino
                    break;
                }
                _ => {
                    // Si no es un token esperado, devuelvo error
                    CustomError::error_invalid_syntax("Expected column name or ')' after '('")?;
                }
            }
        }
    } else {
        CustomError::error_invalid_syntax("Expected '(' after table name")?;
    }
    if columns.is_empty() {
        CustomError::error_invalid_syntax("Expected at least one column name")?;
    }
    Ok(columns)
}

fn parse_insert_values(
    iter: &mut Peekable<Iter<Token>>,
    columns: &[String],
) -> Result<Vec<HashMap<String, String>>, CustomError> {
    if !matches!(iter.next(), Some(Token::Keyword(keyword)) if keyword.as_str() == "VALUES") {
        // Verifico que haya VALUES
        CustomError::error_invalid_syntax("Expected VALUES after column names")?;
    }
    let mut values = vec![];
    let value = parse_insert_value(iter, columns)?;
    values.push(value); // Parseo el primer valor
    while let Some(Token::Symbol(',')) = iter.peek() {
        // Si lo sigue una coma, parseo otro valor
        iter.next();
        let value = parse_insert_value(iter, columns)?;
        values.push(value);
    }
    Ok(values)
}

fn parse_insert_value(
    iter: &mut Peekable<Iter<Token>>,
    columns: &[String],
) -> Result<HashMap<String, String>, CustomError> {
    let mut row: HashMap<String, String> = HashMap::new(); // Hashmap de un VALUE para devolver: columna -> valor
    if let Some(Token::Symbol('(')) = iter.next() {
        // Verifico que se abra parentesis
        let mut column_index = 0; // Indice de la columna actual
        while let Some(token) = iter.next() {
            // Este ciclo termina al encontrar un ')'
            match token {
                Token::Integer(_) | Token::String(_) => {
                    // Si es un valor, lo agrego al hashmap
                    if let Some(Token::Symbol(')')) | Some(Token::Symbol(',')) = iter.peek() {
                    } else {
                        CustomError::error_invalid_syntax("Expected ',' or ')' after value")?;
                    }
                    if column_index >= columns.len() {
                        // Si hay mas valores que columnas, devuelvo error
                        CustomError::error_invalid_syntax("Too many values for columns")?;
                    }
                    let value = match token {
                        Token::Integer(int) => int.to_string(),
                        Token::String(string) => string.to_string(),
                        _ => {
                            CustomError::error_invalid_syntax("Expected value after '('")?;
                            "".to_string()
                        }
                    };
                    row.insert(columns[column_index].to_string(), value); // Agrego el valor de la columna[i] al hashmap
                    column_index += 1;
                }
                Token::Symbol(',') => {
                    // Si es coma, verifico que su siguiente sea un valor
                    if let Some(Token::Integer(_)) | Some(Token::String(_)) = iter.peek() {
                    } else {
                        CustomError::error_invalid_syntax("Expected value after ','")?;
                    }
                }
                Token::Symbol(')') => {
                    // Si se cierra parentesis, pusheo el hashmap al vector de valores y termino
                    break;
                }
                _ => {
                    CustomError::error_invalid_syntax("Expected value or ')' after '('")?;
                }
            }
        }
    }
    Ok(row)
}

// Functions used to parse CREATE

fn parse_create(tokens: &[Token]) -> Result<ParsedQuery, CustomError> {
    if let Some(Token::Keyword(keyword)) = tokens.get(1) {
        match keyword.as_str() {
            "KEYSPACE" => return parse_create_keyspace(tokens),
            "TABLE" => return parse_create_table(tokens),
            _ => {
                CustomError::error_invalid_syntax(&format!("Invalid command: {}", keyword))?;
            }
        }
    } else {
        CustomError::error_invalid_syntax("Usage: CREATE <KEYSPACE | TABLE> <...>")?;
    }

    let (table_name, columns, partition_key_columns, clustering_key_columns) =
        parse_create_table_variables(tokens)?;
    Ok(ParsedQuery::CreateTable {
        table_name,
        columns,
        partition_key_columns,
        clustering_key_columns,
    })
}

fn parse_create_keyspace(tokens: &[Token]) -> Result<ParsedQuery, CustomError> {
    let (keyspace_name, replication_strategy, replication_factor) =
        parse_create_keyspace_variables(tokens)?;
    Ok(ParsedQuery::CreateKeyspace {
        keyspace_name,
        replication_strategy,
        replication_factor,
    })
}

// Parsea solo si cumple con el siguiente formato:
// CREATE KEYSPACE <keyspace_name> WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : <replication_factor> };
fn parse_create_keyspace_variables(
    tokens: &[Token],
) -> Result<(String, String, String), CustomError> {
    let mut keyspace_name = String::new();
    let mut replication_strategy = String::new();
    let mut replication_factor = String::new();
    let mut iter = tokens.iter().peekable();
    iter.next(); // salteo el CREATE
    if !matches!(iter.next(), Some(Token::Keyword(keyword)) if keyword.as_str() == "KEYSPACE") {
        CustomError::error_invalid_syntax("Expected KEYSPACE after CREATE")?;
    }
    if let Some(Token::Identifier(name)) | Some(Token::String(name)) = iter.next() {
        keyspace_name = name.to_string();
    } else {
        CustomError::error_invalid_syntax("Expected keyspace name after KEYSPACE")?;
    }
    if !matches!(iter.next(), Some(Token::Keyword(keyword)) if keyword.as_str() == "WITH") {
        CustomError::error_invalid_syntax("Expected WITH after keyspace name")?;
    }
    if !matches!(iter.next(), Some(Token::Keyword(keyword)) if keyword.as_str() == "REPLICATION") {
        CustomError::error_invalid_syntax("Expected REPLICATION after WITH")?;
    }
    if !matches!(iter.next(), Some(Token::ComparisonOperator(operator)) if operator.as_str() == "=")
    {
        CustomError::error_invalid_syntax("Expected '=' after REPLICATION")?;
    }
    if !matches!(iter.next(), Some(Token::Symbol('{'))) {
        CustomError::error_invalid_syntax("Expected '{' after '='")?;
    }
    if let Some(Token::String(class)) = iter.next() {
        replication_strategy = class.to_string();
    } else {
        CustomError::error_invalid_syntax("Expected 'class' after {")?;
    }
    if !matches!(iter.next(), Some(Token::Symbol(':'))) {
        CustomError::error_invalid_syntax("Expected ':' after 'class'")?;
    }
    if let Some(Token::String(strategy)) = iter.next() {
        replication_strategy = strategy.to_string();
    } else {
        CustomError::error_invalid_syntax("Expected replication strategy after ':'")?;
    }
    if !matches!(iter.next(), Some(Token::Symbol(','))) {
        // Verifico que haya ','
        CustomError::error_invalid_syntax("Expected ',' after replication strategy")?;
    }
    if !matches!(iter.next(), Some(Token::String(factor)) if factor.as_str() == "replication_factor")
    {
        CustomError::error_invalid_syntax("Expected 'replication_factor' after ','")?;
    }
    if !matches!(iter.next(), Some(Token::Symbol(':'))) {
        CustomError::error_invalid_syntax("Expected ':' after 'replication_factor'")?;
    }
    if let Some(Token::String(factor)) | Some(Token::Integer(factor)) = iter.next() {
        replication_factor = factor.to_string();
    } else {
        CustomError::error_invalid_syntax("Expected replication factor after ':'")?;
    }
    if !matches!(iter.next(), Some(Token::Symbol('}'))) {
        CustomError::error_invalid_syntax("Expected '}' after replication factor")?;
    }
    check_ending_with_semicolon(&mut iter)?;
    Ok((keyspace_name, replication_strategy, replication_factor))
}

fn parse_create_table(tokens: &[Token]) -> Result<ParsedQuery, CustomError> {
    let (table_name, columns, partition_key_columns, clustering_key_columns) =
        parse_create_table_variables(tokens)?;
    Ok(ParsedQuery::CreateTable {
        table_name,
        columns,
        partition_key_columns,
        clustering_key_columns,
    })
}

#[allow(clippy::type_complexity)]
fn parse_create_table_variables(
    tokens: &[Token],
) -> Result<(String, Vec<(String, String)>, Vec<String>, Vec<String>), CustomError> {
    let mut table_name = String::new();
    let mut iter = tokens.iter().peekable();
    iter.next(); // salteo el CREATE
    if !matches!(iter.next(), Some(Token::Keyword(keyword)) if keyword.as_str() == "TABLE") {
        // Verifico que haya TABLE
        CustomError::error_invalid_syntax("Expected TABLE after CREATE")?;
    }
    if let Some(Token::Identifier(name)) | Some(Token::String(name)) = iter.next() {
        // Verifico que haya nombre de tabla
        table_name = name.to_string();
    } else {
        CustomError::error_invalid_syntax("Expected table name after TABLE")?;
    }
    if !matches!(iter.peek(), Some(Token::Symbol('('))) {
        // Verifico que haya '('
        CustomError::error_invalid_syntax("Expected '(' after table name")?;
    }
    let (columns, partition_key_columns, clustering_key_columns) =
        parse_create_table_columns(&mut iter)?;
    check_ending_with_semicolon(&mut iter)?;
    Ok((
        table_name,
        columns,
        partition_key_columns,
        clustering_key_columns,
    ))
}

#[allow(clippy::type_complexity)]
fn parse_create_table_columns(
    iter: &mut Peekable<Iter<Token>>,
) -> Result<(Vec<(String, String)>, Vec<String>, Vec<String>), CustomError> {
    let mut columns = vec![];
    let mut partition_key_columns = vec![];
    let mut clustering_key_columns = vec![];
    if let Some(Token::Symbol('(')) = iter.next() {
        // Verifico que se abra parentesis
        while let Some(token) = iter.next() {
            // Este ciclo termina al encontrar un ')'
            match token {
                Token::Identifier(name) | Token::String(name) => {
                    // Si name es PRIMARY, se esta definiendo la primary key
                    if name.as_str() == "PRIMARY" {
                        (partition_key_columns, clustering_key_columns) =
                            parse_create_table_primary_key(iter)?;
                        continue;
                    }
                    // Sino debería ser nombre de columna
                    if let Some(Token::Identifier(column_type)) = iter.next() {
                        // Verifico que haya tipo de dato
                        if ["TEXT", "BIGINT", "INT", "UUID", "TIMESTAMP", "FLOAT"]
                            .contains(&column_type.to_uppercase().as_str())
                        {
                            columns.push((name.to_string(), column_type.to_string()));
                            if let Some(Token::Symbol(')')) | Some(Token::Symbol(',')) = iter.peek()
                            {
                            } else {
                                CustomError::error_invalid_syntax(
                                    "Expected ',' or ')' after column name",
                                )?;
                            }
                        } else {
                            CustomError::error_invalid_syntax(format!("Expected data type after column name, supported data types are: TEXT, BIGINT, INT, UUID, TIMESTAMP, FLOAT. Found: {}", column_type).as_str())?;
                        }
                    } else {
                        CustomError::error_invalid_syntax("Expected data type after column name")?;
                    }
                }
                Token::Symbol(',') => {
                    // Si es coma, verifico que su siguiente sea nombre de columna
                    if let Some(Token::Identifier(_)) | Some(Token::String(_)) = iter.peek() {
                    } else {
                        CustomError::error_invalid_syntax("Expected column name after ','")?;
                    }
                }
                Token::Symbol(')') => {
                    // Si se cierra parentesis, termino
                    break;
                }
                _ => {
                    // Si no es un token esperado, devuelvo error
                    CustomError::error_invalid_syntax("Expected column name or ')' after '('")?;
                }
            }
        }
    } else {
        CustomError::error_invalid_syntax("Expected '(' after table name")?;
    }
    if columns.is_empty() {
        CustomError::error_invalid_syntax("Expected at least one column name")?;
    }
    Ok((columns, partition_key_columns, clustering_key_columns))
}

/// Parses the primary key of a CREATE TABLE query assuming that the primary key is defined as PRIMARY KEY ((partition_key_column1, partition_key_column2, ...), clustering_key_column1, clustering_key_column2, ...)
pub fn parse_create_table_primary_key(
    iter: &mut Peekable<Iter<Token>>,
) -> Result<(Vec<String>, Vec<String>), CustomError> {
    let mut partition_key_columns = vec![];
    let mut clustering_key_columns = vec![];
    if !matches!(iter.next(), Some(Token::Identifier(word)) if word.as_str() == "KEY") {
        // Verifico que haya KEY
        CustomError::error_invalid_syntax("Expected KEY after PRIMARY")?;
    }
    if !matches!(iter.next(), Some(Token::Symbol('('))) {
        // Verifico que haya '('
        CustomError::error_invalid_syntax("Expected '(' after PRIMARY KEY")?;
    }

    // Parseo las partition key columns
    if let Some(Token::Symbol('(')) = iter.next() {
        // Las partition key columns llegaron entre parentesis
        // Verifico que se abra parentesis
        while let Some(token) = iter.next() {
            // Este ciclo termina al encontrar un ')'
            match token {
                Token::Identifier(name) | Token::String(name) => {
                    // Si es nombre de columna, lo agrego
                    partition_key_columns.push(name.to_string());
                    if let Some(Token::Symbol(')')) | Some(Token::Symbol(',')) = iter.peek() {
                    } else {
                        CustomError::error_invalid_syntax("Expected ',' or ')' after column name")?;
                    }
                }
                Token::Symbol(',') => {
                    // Si es coma, verifico que su siguiente sea nombre de columna
                    if let Some(Token::Identifier(_)) | Some(Token::String(_)) = iter.peek() {
                    } else {
                        CustomError::error_invalid_syntax("Expected column name after ','")?;
                    }
                }
                Token::Symbol(')') => {
                    // Si se cierra parentesis, termino
                    break;
                }
                _ => {
                    // Si no es un token esperado, devuelvo error
                    CustomError::error_invalid_syntax("Expected column name or ')' after '('")?;
                }
            }
        }
    } else if let Some(Token::Identifier(name)) = iter.next() {
        // Si no hay parentesis, solo hay una partition key column
        partition_key_columns.push(name.to_string());
    }

    // Parseo las clustering key columns
    while let Some(token) = iter.next() {
        // Este ciclo termina al encontrar un ')'
        match token {
            Token::Identifier(name) | Token::String(name) => {
                // Si es nombre de columna, lo agrego
                clustering_key_columns.push(name.to_string());
                if let Some(Token::Symbol(')')) | Some(Token::Symbol(',')) = iter.peek() {
                } else {
                    CustomError::error_invalid_syntax("Expected ',' or ')' after column name")?;
                }
            }
            Token::Symbol(',') => {
                // Si es coma, verifico que su siguiente sea nombre de columna
                if let Some(Token::Identifier(_)) | Some(Token::String(_)) = iter.peek() {
                } else {
                    CustomError::error_invalid_syntax("Expected column name after ','")?;
                }
            }
            Token::Symbol(')') => {
                // Si se cierra parentesis, termino
                break;
            }
            _ => {
                // Si no es un token esperado, devuelvo error
                CustomError::error_invalid_syntax("Expected column name or ')' after '('")?;
            }
        }
    }
    if partition_key_columns.is_empty() {
        CustomError::error_invalid_syntax("Expected at least one partition key column name")?;
    }
    if clustering_key_columns.is_empty() {
        CustomError::error_invalid_syntax("Expected at least one clustering key column name")?;
    }
    Ok((partition_key_columns, clustering_key_columns))
}

// Functions used to parse UPDATE

fn parse_update(tokens: &[Token]) -> Result<ParsedQuery, CustomError> {
    let (table_name, set_values, condition) = parse_update_variables(tokens)?;
    let query = ParsedQuery::Update {
        table_name: table_name.clone(),
        values_to_update: set_values.clone(),
        condition,
    };
    Ok(query)
}

fn parse_update_variables(
    tokens: &[Token],
) -> Result<(String, HashMap<String, String>, Expression), CustomError> {
    let mut table_name = String::new();

    let mut iter = tokens.iter().peekable();
    iter.next(); // salteo el UPDATE
    if let Some(Token::Identifier(name)) | Some(Token::String(name)) = iter.next() {
        // Verifico que haya nombre de tabla
        table_name = name.to_string();
    } else {
        CustomError::error_invalid_syntax("Expected table name after UPDATE")?;
    }
    let set_values = parse_update_set_values(&mut iter)?;
    let condition = parse_condition(&mut iter)?;
    check_ending_with_semicolon(&mut iter)?;
    Ok((table_name, set_values, condition))
}

fn parse_update_set_values(
    iter: &mut Peekable<Iter<Token>>,
) -> Result<HashMap<String, String>, CustomError> {
    let mut set_values: HashMap<String, String> = HashMap::new();
    if !matches!(iter.next(), Some(Token::Keyword(keyword)) if keyword.as_str() == "SET") {
        // Verifico que haya SET
        CustomError::error_invalid_syntax("Expected SET after table name")?;
    }
    let (column, value) = parse_update_set_value(iter)?; // Parseo el primer valor
    set_values.insert(column, value);
    while let Some(Token::Symbol(',')) = iter.peek() {
        // Si lo sigue una coma, parseo otro valor
        iter.next();
        let (column, value) = parse_update_set_value(iter)?;
        set_values.insert(column, value);
    }
    Ok(set_values)
}

fn parse_update_set_value(
    iter: &mut Peekable<Iter<Token>>,
) -> Result<(String, String), CustomError> {
    let mut column: String = "".to_string();
    let mut value: String = "".to_string();
    if let Some(Token::Identifier(name)) | Some(Token::String(name)) = iter.next() {
        // Verifico que haya nombre de columna
        column = name.to_string();
    } else {
        CustomError::error_invalid_syntax("Expected column name to set value after SET")?;
    }
    if matches!(iter.next(), Some(Token::ComparisonOperator(keyword)) if keyword.as_str() == "=") {
        // Verifico que haya '='
        if let Some(Token::Integer(string)) | Some(Token::String(string)) = iter.next() {
            // Verifico que haya valor
            value = string.to_string();
        } else {
            CustomError::error_invalid_syntax("Expected value after '='")?;
        }
    } else {
        CustomError::error_invalid_syntax("Expected '=' after column name")?;
    }
    Ok((column, value))
}

// Functions used to parse DELETE

fn parse_delete(tokens: &[Token]) -> Result<ParsedQuery, CustomError> {
    let (table_name, condition) = parse_delete_variables(tokens)?;
    Ok(ParsedQuery::Delete {
        table_name,
        condition,
    })
}

fn parse_delete_variables(tokens: &[Token]) -> Result<(String, Expression), CustomError> {
    let mut table_name = String::new();
    let mut iter = tokens.iter().peekable();
    iter.next(); // salteo el DELETE
    if !matches!(iter.next(), Some(Token::Keyword(keyword)) if keyword.as_str() == "FROM") {
        // Verifico que haya FROM
        CustomError::error_invalid_syntax("Expected FROM after DELETE")?;
    }
    if let Some(Token::Identifier(name)) | Some(Token::String(name)) = iter.next() {
        // Verifico que haya nombre de tabla
        table_name = name.to_string();
    } else {
        CustomError::error_invalid_syntax("Expected table name after FROM")?;
    }
    let condition = parse_condition(&mut iter)?;
    check_ending_with_semicolon(&mut iter)?;
    Ok((table_name, condition))
}

// Functions used to parse SELECT

fn parse_select(tokens: &[Token]) -> Result<ParsedQuery, CustomError> {
    let (table_name, columns, condition, order_by) = parse_select_variables(tokens)?;
    Ok(ParsedQuery::Select {
        table_name,
        columns,
        condition,
        order_by,
    })
}

#[allow(clippy::type_complexity)]
fn parse_select_variables(
    tokens: &[Token],
) -> Result<(String, Vec<String>, Expression, Vec<(String, String)>), CustomError> {
    let mut iter = tokens.iter().peekable();
    iter.next(); // salteo el SELECT
    let columns = parse_select_columns(&mut iter)?;
    let table_name = parse_select_from(&mut iter)?;
    let condition = parse_condition(&mut iter)?;
    let order_by = parse_order_by(&mut iter)?;
    check_ending_with_semicolon(&mut iter)?;
    Ok((table_name, columns, condition, order_by))
}

fn parse_select_columns(iter: &mut Peekable<Iter<Token>>) -> Result<Vec<String>, CustomError> {
    let mut columns = vec![];

    if matches!(iter.peek(), Some(Token::Symbol('*'))) {
        // Si hay '*', lo dejo vacío, que indica que se seleccionan todas las columnas
        iter.next();
        return Ok(columns);
    }
    while let Some(token) = iter.peek() {
        // Este ciclo termina al encontrar un Keyword
        match token {
            Token::Identifier(name) | Token::String(name) => {
                // Si es nombre de columna, lo agrego
                columns.push(name.to_string());
                iter.next();
            }
            Token::Keyword(_) => {
                // Si es Keyword, termino
                break;
            }
            Token::Symbol(',') => {
                // Si es coma, verifico que su siguiente sea nombre de columna
                iter.next();
                if let Some(Token::Identifier(_)) | Some(Token::String(_)) = iter.peek() {
                } else {
                    CustomError::error_invalid_syntax("Expected column name after ','")?;
                }
            }
            _ => {
                CustomError::error_invalid_syntax(
                    "Expected column name or FROM <tablename> after column names",
                )?;
            }
        }
    }
    Ok(columns)
}

fn parse_select_from(iter: &mut Peekable<Iter<Token>>) -> Result<String, CustomError> {
    let mut table_name = String::new();
    if !matches!(iter.next(), Some(Token::Keyword(keyword)) if keyword.as_str() == "FROM") {
        // Verifico que haya FROM
        CustomError::error_invalid_syntax("Expected FROM after column names")?;
    }
    if let Some(Token::Identifier(name)) | Some(Token::String(name)) = iter.next() {
        // Verifico que haya nombre de tabla
        table_name = name.to_string();
    } else {
        CustomError::error_invalid_syntax("Expected table name after FROM")?;
    }
    Ok(table_name)
}

fn parse_order_by(iter: &mut Peekable<Iter<Token>>) -> Result<Vec<(String, String)>, CustomError> {
    let mut order_by = vec![];
    if matches!(iter.peek(), Some(Token::Keyword(keyword)) if keyword.as_str() == "ORDER") {
        // Verifico que haya ORDER
        iter.next();
        if !matches!(iter.next(), Some(Token::Keyword(keyword)) if keyword.as_str() == "BY") {
            // Verifico que haya BY
            CustomError::error_invalid_syntax("Expected BY after ORDER")?;
        }
    } else {
        // Si no hay ORDER BY, no hay ningun orden que seguir
        return Ok(order_by);
    }
    let (column, asc_or_desc) = parse_order_by_column(iter)?; // Parseo la primera columna por la cual ordenar
    order_by.push((column, asc_or_desc));
    while let Some(Token::Symbol(',')) = iter.peek() {
        // Si lo sigue una coma, parseo otra columna
        iter.next();
        let (column, asc_or_desc) = parse_order_by_column(iter)?; // Parseo la primera columna por la cual ordenar
        order_by.push((column, asc_or_desc));
    }
    Ok(order_by)
}

fn parse_order_by_column(
    iter: &mut Peekable<Iter<Token>>,
) -> Result<(String, String), CustomError> {
    let _order_by_tuple: (String, String);
    let order_by_column: String;
    if let Some(Token::Identifier(name)) | Some(Token::String(name)) = iter.next() {
        // Verifico que haya nombre de columna
        order_by_column = name.to_string();
        if let Some(Token::Keyword(keyword)) = iter.peek() {
            // Verifico que haya DESC o nada
            if keyword.as_str() == "DESC" {
                iter.next();
                return Ok((order_by_column, "DESC".to_string()));
            } else if keyword.as_str() == "ASC" {
                iter.next();
                return Ok((order_by_column, "ASC".to_string()));
            } else {
                CustomError::error_invalid_syntax(
                    "Expected DESC, ASC or nothing after column name",
                )?;
            }
        } else {
            return Ok((order_by_column, "ASC".to_string()));
        }
    } else {
        CustomError::error_invalid_syntax("Expected column name after ORDER BY or ','")?;
    }
    Ok(("".to_string(), "".to_string()))
}

// Functions used to check global syntax

fn check_ending_with_semicolon(iter: &mut Peekable<Iter<Token>>) -> Result<(), CustomError> {
    if let Some(Token::Symbol(';')) = iter.next() {
        if iter.peek().is_some() {
            return CustomError::error_invalid_syntax("Tokens found after ';'");
        }
    } else {
        return CustomError::error_invalid_syntax("Expected ';' at the end of the command");
    }
    Ok(())
}

fn parse_condition(iter: &mut Peekable<Iter<Token>>) -> Result<Expression, CustomError> {
    if let Some(Token::Keyword(keyword)) = iter.peek() {
        // Verifico que haya WHERE
        if keyword.as_str() == "WHERE" {
            iter.next();
            parse_expression(iter)
        } else {
            Ok(Expression::True)
        }
    } else {
        Ok(Expression::True)
    }
}

#[cfg(test)]
mod tests {
    use expression::Operand;

    //use super::expression::Operand;
    use super::*;

    #[test]
    fn test_parse_insert() {
        let query = "INSERT INTO table1 (column1, column2) VALUES (1, 'value1'), (2, 'value2');";
        let instruction = parse_instruction(query).unwrap();
        if let ParsedQuery::Insert {
            table_name,
            columns_in_order,
            rows_to_insert,
        } = &instruction
        {
            assert_eq!(table_name, "table1");
            assert_eq!(rows_to_insert.len(), 2);
            assert_eq!(rows_to_insert[0].get("column1").unwrap(), &"1".to_string());
            assert_eq!(
                rows_to_insert[0].get("column2").unwrap(),
                &"value1".to_string()
            );
            assert_eq!(rows_to_insert[1].get("column1").unwrap(), &"2".to_string());
            assert_eq!(
                rows_to_insert[1].get("column2").unwrap(),
                &"value2".to_string()
            );
            assert_eq!(columns_in_order, &["column1".to_string(), "column2".to_string()]);
        } else {
            panic!("Expected Insert instruction");
        }
    }

    #[test]
    fn test_parse_insert_invalid_syntax() {
        let query = "INSERT INTO table1 (column1, column2) VALUES (1, 'value1', 2, 'value2');";
        let result = parse_instruction(query);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_delete() {
        let query = "DELETE FROM table1 WHERE column1 = 1;";
        let instruction = parse_instruction(query).unwrap();
        if let ParsedQuery::Delete {
            table_name,
            condition,
        } = &instruction
        {
            assert_eq!(table_name, "table1");
            assert_eq!(
                condition,
                &Expression::Comparison {
                    left: Operand::Column("column1".to_string()),
                    right: Operand::Integer("1".to_string()),
                    operator: '='.to_string()
                }
            );
        } else {
            panic!("Expected Delete instruction");
        }
    }

    #[test]
    fn test_parse_delete_with_and() {
        let query = "DELETE FROM table1 WHERE column1 = 1 AND column2 = 'value';";
        let instruction = parse_instruction(query).unwrap();
        if let ParsedQuery::Delete {
            table_name,
            condition,
        } = &instruction
        {
            assert_eq!(table_name, "table1");
            assert_eq!(
                condition,
                &Expression::And {
                    left: Box::new(Expression::Comparison {
                        left: Operand::Column("column1".to_string()),
                        right: Operand::Integer("1".to_string()),
                        operator: '='.to_string()
                    }),
                    right: Box::new(Expression::Comparison {
                        left: Operand::Column("column2".to_string()),
                        right: Operand::String("value".to_string()),
                        operator: '='.to_string()
                    })
                }
            );
        } else {
            panic!("Expected Delete instruction");
        }
    }
}
