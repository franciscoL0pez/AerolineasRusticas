use crate::frame::messages::error::ErrorCode;
use crate::frame::messages::query::Query;
use crate::frame::messages::query_result::QueryResult;
use crate::frame::messages::startup_options::{
    deserialize_options, deserialize_startup, serialize_options, serialize_startup,
};
use std::io;

pub mod authentication;
pub mod consistency_level;
pub mod error;
mod notation;
pub mod query;
pub mod query_result;
pub mod startup_options;

#[repr(u8)]
#[derive(Debug, Clone)]
pub enum Message {
    Error(ErrorCode) = 0x00, // = op code
    Startup(Vec<(String, String)>) = 0x01,
    Ready = 0x02,
    Authenticate(String) = 0x03, // IAuthenticator
    Options = 0x05,
    Supported(Vec<(String, Vec<String>)>) = 0x06,
    Query(Query) = 0x07,
    Result(QueryResult) = 0x08,
    Prepare = 0x09,
    Execute = 0x0A,
    Register = 0x0B,
    Event = 0x0C,
    Batch = 0x0D,
    AuthChallenge(Vec<u8>) = 0x0E,
    AuthResponse(Vec<u8>) = 0x0F,
    AuthSuccess = 0x10,
}

impl Message {
    pub(crate) fn deserialize(op_code: u8, body: Vec<u8>) -> io::Result<Self> {
        match op_code {
            0x00 => Ok(Message::Error(ErrorCode::deserialize_to_code(&body)?)),
            0x01 => Ok(Message::Startup(deserialize_startup(&body)?)),
            0x02 => Ok(Message::Ready),

            0x03 => Ok(Message::Authenticate(
                authentication::deserialize_authenticate(&body),
            )),

            0x05 => Ok(Message::Options),
            0x06 => Ok(Message::Supported(deserialize_options(&body)?)),
            0x07 => Ok(Message::Query(Query::deserialize(&body)?)),
            0x08 => Ok(Message::Result(QueryResult::deserialize(&body)?)),
            0x09 => Ok(Message::Prepare),
            0x0A => Ok(Message::Execute),
            0x0B => Ok(Message::Register),
            0x0C => Ok(Message::Event),
            0x0D => Ok(Message::Batch),
            0x0E => Ok(Message::AuthChallenge(body)),
            0x0F => Ok(Message::AuthResponse(body)),
            0x10 => Ok(Message::AuthSuccess),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Unknown operation code",
            )),
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        match self {
            Message::Error(error_code) => error_code.serialize(),
            Message::Startup(options_selected) => serialize_startup(options_selected),
            Message::Supported(options) => serialize_options(options),
            Message::Query(query) => query.serialize(),
            Message::Result(query_result) => query_result.serialize(),

            Message::Authenticate(iauthenticator) => {
                authentication::serialize_authenticate(iauthenticator)
            }

            Message::AuthChallenge(auth_challenge) => auth_challenge.to_vec(),
            Message::AuthResponse(auth_response) => auth_response.to_vec(),
            _ => vec![],
        }
    }

    pub fn to_op_code(&self) -> u8 {
        match self {
            Message::Error(_) => 0x00,
            Message::Startup(_) => 0x01,
            Message::Ready => 0x02,
            Message::Authenticate(_) => 0x03,
            Message::Options => 0x05,
            Message::Supported(_) => 0x06,
            Message::Query(_) => 0x07,
            Message::Result(_) => 0x08,
            Message::Prepare => 0x09,
            Message::Execute => 0x0A,
            Message::Register => 0x0B,
            Message::Event => 0x0C,
            Message::Batch => 0x0D,
            Message::AuthChallenge(_) => 0x0E,
            Message::AuthResponse(_) => 0x0F,
            Message::AuthSuccess => 0x10,
        }
    }
}
