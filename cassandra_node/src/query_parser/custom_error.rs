use std::fmt;

#[derive(Debug, PartialEq)]
/// Tipo de error personalizado para el programa.
pub enum CustomError {
    /// Error relacionados con lecctura de tablas.
    InvalidTable { message: String },
    /// Error relacionados con columnas del comando.
    InvalidColumn { message: String },
    /// Error relacionados con sintaxis del comando.
    InvalidSyntax { message: String },
    /// Error genérico.
    GenericError { message: String },
}

impl fmt::Display for CustomError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CustomError::InvalidTable { message } => write!(f, "INVALID_TABLE: {}", message),
            CustomError::InvalidColumn { message } => write!(f, "INVALID_COLUMN: {}", message),
            CustomError::InvalidSyntax { message } => write!(f, "INVALID_SYNTAX: {}", message),
            CustomError::GenericError { message } => write!(f, "ERROR: {}", message),
        }
    }
}

impl CustomError {
    pub fn error_invalid_syntax(message: &str) -> Result<(), CustomError> {
        Err(CustomError::InvalidSyntax {
            message: message.to_string(),
        })
    }

    pub fn error_invalid_table(message: &str) -> Result<(), CustomError> {
        Err(CustomError::InvalidTable {
            message: message.to_string(),
        })
    }

    pub fn error_invalid_column(message: &str) -> Result<(), CustomError> {
        Err(CustomError::InvalidColumn {
            message: message.to_string(),
        })
    }

    pub fn error_generic(message: &str) -> Result<(), CustomError> {
        Err(CustomError::GenericError {
            message: message.to_string(),
        })
    }
}
