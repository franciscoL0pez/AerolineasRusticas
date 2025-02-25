use crate::frame::messages::error::ErrorCode;
use crate::frame::messages::startup_options::{
    default_supported, validate_options,
};
use crate::frame::messages::Message;
use crate::frame::version::Version;
use crate::security::EncryptionHandler;
use std::sync::Arc;

use super::messages::authentication::{AuthChallenge, AuthResponse};
use super::messages::query::Query;
use super::messages::query_result::QueryResult;
use super::Frame;

#[derive(Debug, PartialEq)]
pub enum ConnectionState {
    Uninitialized,
    UnAuthenticated,
    Authenticating,
    Ready,
}

pub trait Node {
    fn resend_query_as_internal_message(
        &self,
        query: Query,
        keyspace: Option<String>,
    ) -> Result<QueryResult, ErrorCode>;
}

impl Frame {
    pub fn new_server_error() -> Self {
        Frame::new_error(ErrorCode::ServerError, 0)
    }

    pub fn generate_response(&self, node: Arc<dyn Node>, keyspace: &mut Option<String>) -> Self {
        let body = match &self.body {
            Message::Query(query) => {
                let response =
                    node.resend_query_as_internal_message(query.clone(), keyspace.clone());
                match response {
                    Ok(query_result) => {
                        if let QueryResult::SetKeyspace(keyspace_name) = &query_result {
                            *keyspace = Some(keyspace_name.clone());

                            println!("Keyspace set to: {}", keyspace_name);
                        }
                        Message::Result(query_result)
                    }
                    Err(error_code) => Message::Error(error_code),
                }
            }
            Message::Error(error) => Message::Error(*error),
            _ => Message::Error(ErrorCode::ProtocolError),
        };

        Self {
            version: Version::ResponseV3,
            compression: self.compression,
            tracing: self.tracing,
            stream: self.stream,
            body,
        }
    }

    pub fn handle_uninitialized(&self, conncection_state: &mut ConnectionState) -> Self {
        let body = match &self.body {
            Message::Startup(selected_options) => {
                if !validate_options(selected_options) {
                    Message::Error(ErrorCode::ProtocolError)
                } else {
                    *conncection_state = ConnectionState::UnAuthenticated;
                    Message::Authenticate("PLAIN".to_string())
                }
            }
            Message::Options => Message::Supported(default_supported()),
            _ => Message::Error(ErrorCode::ProtocolError),
        };

        Self {
            version: Version::ResponseV3,
            compression: self.compression,
            tracing: self.tracing,
            stream: self.stream,
            body,
        }
    }

    pub fn handle_authentication(
        &self,
        connection_state: &mut ConnectionState,
        encryption_handler: &mut EncryptionHandler,
    ) -> Self {
        let body = match &self.body {
            Message::AuthResponse(response) => {
                authenticate_client(response, connection_state, encryption_handler)
            }

            _ => Message::Error(ErrorCode::ProtocolError),
        };

        Self {
            version: Version::ResponseV3,
            compression: self.compression,
            tracing: self.tracing,
            stream: self.stream,
            body,
        }
    }
}

fn authenticate_client(
    auth_response: &[u8],
    connection_state: &mut ConnectionState,
    encryption_handler: &mut EncryptionHandler,
) -> Message {
    match *connection_state {
        ConnectionState::UnAuthenticated => {
            *connection_state = ConnectionState::Authenticating;

            let (public_key, prime, base) = encryption_handler.get_dh_params();

            let challenge = AuthChallenge::new(public_key, prime, base);

            Message::AuthChallenge(challenge.serialize())
        }
        ConnectionState::Authenticating => {
            let response = AuthResponse::deserialize(auth_response);

            match encryption_handler.attempt_initialize(response.public_key, response.shared_secret)
            {
                true => {
                    *connection_state = ConnectionState::Ready;
                    Message::AuthSuccess
                }
                false => {
                    *connection_state = ConnectionState::Uninitialized;
                    Message::Error(ErrorCode::BadCredentials)
                }
            }
        }

        _ => Message::Error(ErrorCode::ServerError),
    }
}