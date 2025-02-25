use crate::frame::messages::notation::{read_int, write_string};
use std::io;
use std::io::Cursor;

/// ```ignore
/// 9. Error codes
///
///   The supported error codes are described below:
///     0x0000    Server error: something unexpected happened. This indicates a
///               server-side bug.
///     0x000A    Protocol error: some client message triggered a protocol
///               violation (for instance a QUERY message is sent before a STARTUP
///               one has been sent)
///     0x0100    Bad credentials: CREDENTIALS request failed because Cassandra
///               did not accept the provided credentials.
///
///     0x1000    Unavailable exception. The rest of the ERROR message body will be
///                 <cl><required><alive>
///               where:
///                 <cl> is the [consistency] level of the query having triggered
///                      the exception.
///                 <required> is an [int] representing the number of node that
///                            should be alive to respect <cl>
///                 <alive> is an [int] representing the number of replica that
///                         were known to be alive when the request has been
///                         processed (since an unavailable exception has been
///                         triggered, there will be <alive> < <required>)
///     0x1001    Overloaded: the request cannot be processed because the
///               coordinator node is overloaded
///     0x1002    Is_bootstrapping: the request was a read request but the
///               coordinator node is bootstrapping
///     0x1003    Truncate_error: error during a truncation error.
///     0x1100    Write_timeout: Timeout exception during a write request. The rest
///               of the ERROR message body will be
///                 <cl><received><blockfor><writeType>
///               where:
///                 <cl> is the [consistency] level of the query having triggered
///                      the exception.
///                 <received> is an [int] representing the number of nodes having
///                            acknowledged the request.
///                 <blockfor> is an [int] representing the number of replica whose
///                            acknowledgement is required to achieve <cl>.
///                 <writeType> is a [string] that describe the type of the write
///                             that timeouted. The value of that string can be one
///                             of:
///                              - "SIMPLE": the write was a non-batched
///                                non-counter write.
///                              - "BATCH": the write was a (logged) batch write.
///                                If this type is received, it means the batch log
///                                has been successfully written (otherwise a
///                                "BATCH_LOG" type would have been send instead).
///                              - "UNLOGGED_BATCH": the write was an unlogged
///                                batch. Not batch log write has been attempted.
///                              - "COUNTER": the write was a counter write
///                                (batched or not).
///                              - "BATCH_LOG": the timeout occured during the
///                                write to the batch log when a (logged) batch
///                                write was requested.
///                              - "CAS": the timeout occured during the Compare And Set write/update.
///     0x1200    Read_timeout: Timeout exception during a read request. The rest
///               of the ERROR message body will be
///                 <cl><received><blockfor><data_present>
///               where:
///                 <cl> is the [consistency] level of the query having triggered
///                      the exception.
///                 <received> is an [int] representing the number of nodes having
///                            answered the request.
///                 <blockfor> is an [int] representing the number of replica whose
///                            response is required to achieve <cl>. Please note that
///                            it is possible to have <received> >= <blockfor> if
///                            <data_present> is false. And also in the (unlikely)
///                            case were <cl> is achieved but the coordinator node
///                            timeout while waiting for read-repair
///                            acknowledgement.
///                 <data_present> is a single byte. If its value is 0, it means
///                                the replica that was asked for data has not
///                                responded. Otherwise, the value is != 0.
///
///     0x2000    Syntax_error: The submitted query has a syntax error.
///     0x2100    Unauthorized: The logged user doesn't have the right to perform
///               the query.
///     0x2200    Invalid: The query is syntactically correct but invalid.
///     0x2300    Config_error: The query is invalid because of some configuration issue
///     0x2400    Already_exists: The query attempted to create a keyspace or a
///               table that was already existing. The rest of the ERROR message
///               body will be <ks><table> where:
///                 <ks> is a [string] representing either the keyspace that
///                      already exists, or the keyspace in which the table that
///                      already exists is.
///                 <table> is a [string] representing the name of the table that
///                         already exists. If the query was attempting to create a
///                         keyspace, <table> will be present but will be the empty
///                         string.
///     0x2500    Unprepared: Can be thrown while a prepared statement tries to be
///               executed if the provide prepared statement ID is not known by
///               this host. The rest of the ERROR message body will be [short
///               bytes] representing the unknown ID.
/// ```

//const UNKNOWN: &str = "Unknown error code";
#[repr(i32)]
#[derive(Copy, Clone, Debug)]
pub enum ErrorCode {
    ServerError = 0x0000,
    ProtocolError = 0x000A,
    BadCredentials = 0x0100,
    UnavailableException = 0x1000,
    Overloaded = 0x1001,
    IsBootstrapping = 0x1002,
    TruncateError = 0x1003,
    WriteTimeout = 0x1100,
    ReadTimeout = 0x1200,
    SyntaxError = 0x2000,
    Unauthorized = 0x2100,
    Invalid = 0x2200,
    ConfigError = 0x2300,
    AlreadyExists = 0x2400,
    Unprepared = 0x2500,
}

impl TryFrom<i32> for ErrorCode {
    type Error = io::Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0x0000 => Ok(ErrorCode::ServerError),
            0x000A => Ok(ErrorCode::ProtocolError),
            0x0100 => Ok(ErrorCode::BadCredentials),
            0x1000 => Ok(ErrorCode::UnavailableException),
            0x1001 => Ok(ErrorCode::Overloaded),
            0x1002 => Ok(ErrorCode::IsBootstrapping),
            0x1003 => Ok(ErrorCode::TruncateError),
            0x1100 => Ok(ErrorCode::WriteTimeout),
            0x1200 => Ok(ErrorCode::ReadTimeout),
            0x2000 => Ok(ErrorCode::SyntaxError),
            0x2100 => Ok(ErrorCode::Unauthorized),
            0x2200 => Ok(ErrorCode::Invalid),
            0x2300 => Ok(ErrorCode::ConfigError),
            0x2400 => Ok(ErrorCode::AlreadyExists),
            0x2500 => Ok(ErrorCode::Unprepared),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Unknown operation code",
            )),
        }
    }
}

impl ErrorCode {
    pub fn serialize(&self) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&i32::from(*self).to_be_bytes());
        write_string(&mut body, self.message());
        body
    }

    pub fn deserialize_to_code(body: &[u8]) -> io::Result<Self> {
        let mut cursor = Cursor::new(body);
        let code = read_int(&mut cursor)?;
        ErrorCode::try_from(code)
    }

    pub fn message(&self) -> &'static str {
        match self {
            ErrorCode::ServerError => "A server error occurred.",
            ErrorCode::ProtocolError => "There was a protocol error.",
            ErrorCode::BadCredentials => "Invalid credentials provided.",
            ErrorCode::UnavailableException => "The requested service is unavailable.",
            ErrorCode::Overloaded => "The server is overloaded.",
            ErrorCode::IsBootstrapping => "The server is currently bootstrapping.",
            ErrorCode::TruncateError => "An error occurred while truncating data.",
            ErrorCode::WriteTimeout => "A write timeout occurred.",
            ErrorCode::ReadTimeout => "A read timeout occurred.",
            ErrorCode::SyntaxError => "There is a syntax error in the query.",
            ErrorCode::Unauthorized => "You are unauthorized to perform this action.",
            ErrorCode::Invalid => "The request was invalid.",
            ErrorCode::ConfigError => "There is a configuration error.",
            ErrorCode::AlreadyExists => "The item you are trying to create already exists.",
            ErrorCode::Unprepared => "The query was not prepared.",
        }
    }
}

impl From<ErrorCode> for i32 {
    fn from(code: ErrorCode) -> Self {
        code as i32
    }
}
