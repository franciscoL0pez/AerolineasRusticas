use std::io;

use crate::frame::messages::consistency_level::ConsistencyLevel;
use crate::frame::messages::startup_options::default_startup;
use crate::frame::messages::{query, Message};
use crate::frame::version::Version;
use crate::frame::Frame;

impl Frame {
    pub fn new_startup() -> Self {
        Self {
            version: Version::RequestV3,
            compression: false,
            tracing: false,
            stream: rand::random(),
            body: Message::Startup(default_startup()),
        }
    }

    // Deprecated because of authentication
    pub fn is_valid_ready(&self, startup: Self) -> Result<i16, String> {
        if self.stream != startup.stream {
            return Err("Received invalid stream id".to_string());
        }
        match self.body {
            Message::Ready => Ok(self.stream),
            Message::Error(error_code) => Err(error_code.message().to_string()),
            _ => Err("Server is not ready. Unknown protocol error.".to_string()),
        }
    }

    pub fn new_query(
        query_string: String,
        consistency_level: ConsistencyLevel,
        stream_id: i16,
    ) -> Self {
        let query = query::Query::default(query_string, consistency_level);
        Self {
            version: Version::RequestV3,
            compression: false,
            tracing: false,
            stream: stream_id,
            body: Message::Query(query),
        }
    }

    pub fn handle_response(&self, query: Self) -> Result<String, String> {
        if self.version != Version::ResponseV3 {
            return Err("Invalid version".to_string());
        }
        if self.stream != query.stream {
            return Err("Invalid stream id".to_string());
        };
        match &self.body {
            Message::Result(result) => Ok(result.to_string()),
            Message::Error(error_code) => Err(error_code.message().to_string()),
            _ => Err("Unknown protocol error.".to_string()),
        }
    }

    pub fn get_authenticator(&self) -> io::Result<(String, i16)> {
        if self.version != Version::ResponseV3 {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                "Invalid version",
            ));
        }

        match &self.body {
            Message::Authenticate(authenticator) => Ok((authenticator.clone(), self.stream)),
            Message::Error(error_code) => Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                error_code.message(),
            )),
            _ => Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                "Unknown protocol error",
            )),
        }
    }

    pub fn get_auth_challenge(&self) -> io::Result<Vec<u8>> {
        if self.version != Version::ResponseV3 {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                "Invalid version",
            ));
        }

        match &self.body {
            Message::AuthChallenge(challenge) => Ok(challenge.clone()),
            Message::Error(error_code) => Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                error_code.message(),
            )),
            _ => Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                "Unknown protocol error",
            )),
        }
    }

    pub fn new_auth_response(&self, response: Vec<u8>) -> Self {
        Self {
            version: Version::RequestV3,
            compression: self.compression,
            tracing: self.tracing,
            stream: self.stream,
            body: Message::AuthResponse(response),
        }
    }

    pub fn is_success(&self) -> bool {
        matches!(&self.body, Message::AuthSuccess)
    }
}
