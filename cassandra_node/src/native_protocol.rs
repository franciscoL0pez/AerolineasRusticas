use common::frame::Frame;
use common::security::EncryptionHandler;
use common::frame::server_handle::ConnectionState;

use crate::node::Node;
use std::io;
use std::io::Error;
use std::net::TcpStream;
use std::sync::Arc;

/// Attempts to create a new `Connection` from the given `stream`.
/// 
/// # Errors
/// 
/// Returns an error as a `String` if the connection could not be established.
pub fn handle_native_protocol_connection(stream: TcpStream, node: Arc<Node>) -> Result<(), String> {
    let peer_addr = stream.peer_addr().unwrap();

    let mut connection = Connection::new(stream).map_err(|e| e.to_string())?;

    println!("Servidor de native protocol escuchando en {:?}", peer_addr);

    loop {
        connection.connection_loop(Arc::clone(&node))?
    }
}

impl common::frame::server_handle::Node for Node {
    fn resend_query_as_internal_message(
        &self,
        query: common::frame::messages::query::Query,
        keyspace: Option<String>,
    ) -> Result<common::frame::messages::query_result::QueryResult, common::frame::messages::error::ErrorCode> {
        self.resend_query_as_internal_message(query, keyspace)
    }
}

struct Connection {
    stream: TcpStream,
    connection_state: ConnectionState,
    keyspace: Option<String>,
    encryption_handler: EncryptionHandler,
}

impl Connection {
    fn new(stream: TcpStream) -> io::Result<Self> {
        Ok(Self {
            stream,
            connection_state: ConnectionState::Uninitialized,
            keyspace: None,
            encryption_handler: EncryptionHandler::new(23, 5),
        })
    }

    fn read(&mut self) -> io::Result<Frame> {
        self.encryption_handler.read(&mut self.stream)
    }

    fn write(&mut self, frame: &Frame) -> io::Result<()> {
        self.encryption_handler.write(&mut self.stream, frame)
    }

    fn handle_request(&mut self, request: Frame, node: Arc<Node>) -> Result<Frame, String> {
        match self.connection_state {
            ConnectionState::Uninitialized => {
                Ok(request.handle_uninitialized(&mut self.connection_state))
            }
            ConnectionState::Ready => self.generate_response(request, node),
            _ => Ok(request
                .handle_authentication(&mut self.connection_state, &mut self.encryption_handler)),
        }
    }

    fn generate_response(&mut self, request: Frame, node: Arc<Node>) -> Result<Frame, String> {
        Ok(request.generate_response(node, &mut self.keyspace))
    }

    fn connection_loop(&mut self, node: Arc<Node>) -> Result<(), String> {
        match self.read() {
            Ok(request) => {
                let response = self.handle_request(request, node)?;

                if let Err(e) = self.write(&response) {
                    if is_legitimate_error(&e) {
                        println!("Error al enviar respuesta: {}", e);
                        return Err(e.to_string());
                    }
                }

                Ok(())
            }

            Err(e) => self.connection_error(e),
        }
    }

    fn connection_error(&mut self, e: Error) -> Result<(), String> {
        if !is_legitimate_error(&e) {
            return Ok(());
        }
        println!("El cliente ha cerrado la conexión.");
        // println!("Error al leer del stream: {}", e);
        Err(self.write_server_error(e))
    }

    fn write_server_error(&mut self, error: Error) -> String {
        let _ = self.write(&Frame::new_server_error());
        error.to_string()
    }
}

/// Checks if the given error indicates a legitimate problem (not a closure).
///
/// Returns `true` if the error is considered a legitimate problem,
/// and `false` if it indicates a closure or can be ignored.
fn is_legitimate_error(e: &Error) -> bool {
    match e.kind() {
        io::ErrorKind::ConnectionReset
        | io::ErrorKind::ConnectionAborted
        | io::ErrorKind::TimedOut => false, // These errors typically indicate closure or transient issues
        _ => true,
    }
}
