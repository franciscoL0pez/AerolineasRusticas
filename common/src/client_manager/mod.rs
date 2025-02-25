mod auth;

use auth::authenticate_to_server;
use rand::rng;
use rand::seq::SliceRandom;

use crate::frame::messages::consistency_level::ConsistencyLevel;
use crate::frame::Frame;
use crate::security::EncryptionHandler;
use std::io::{self};
use std::net::TcpStream;

const RETRIES: u8 = 3;

#[derive(Debug)]
pub struct ClientManager {
    addresses: Vec<String>,
    stream: TcpStream,
    stream_id: i16,
    encryption_handler: EncryptionHandler,
    current_keyspace: String,
    unanswered_queries: Vec<Frame>,
}

impl ClientManager {
    /// Creates a new `ClientManager` by connecting to the first available address and authenticating.
    pub fn new(addresses: &[String]) -> io::Result<ClientManager> {
        let mut stream = connect_to_first_available(addresses)?;
        println!("Connected to {:?}", stream.peer_addr());

        let (encryption_handler, stream_id) = authenticate_to_server(&mut stream)?;

        Ok(ClientManager {
            addresses: addresses.to_vec(),
            stream,
            stream_id,
            encryption_handler,
            current_keyspace: String::new(),
            unanswered_queries: Vec::new(),
        })
    }

    /// Sets the current keyspace for the client.
    pub fn use_keyspace(&mut self, keyspace: &str) -> Result<(), String> {
        let query = format!("USE {};", keyspace);
        let res_ks = self.query(query, "")?;
        self.current_keyspace = res_ks;
        Ok(())
    }

    /// Executes a query with the given consistency level.
    pub fn query(
        &mut self,
        query_string: String,
        consistency_level: &str,
    ) -> Result<String, String> {
        let consistency_level = ConsistencyLevel::from_str_to_enum(consistency_level);

        let query = Frame::new_query(query_string, consistency_level, self.stream_id);

        let response = self.execute_query(&query).map_err(|e| e.to_string())?;

        response.handle_response(query)
    }

    /// Writes a frame to the server, with support for retries and reconnection.
    pub fn write(&mut self, frame: &Frame) -> io::Result<()> {
        loop {
            let operation = |manager: &mut ClientManager| {
                manager.encryption_handler.write(&mut manager.stream, frame)
            };

            if self.retries(operation).is_ok() {
                self.unanswered_queries.push(frame.clone());
                return Ok(());
            }

            self.reconnect()?;
        }
    }

    /// Reads a frame from the server, with support for retries and reconnection.
    pub fn read(&mut self) -> io::Result<Frame> {
        let operation =
            |manager: &mut ClientManager| manager.encryption_handler.read(&mut manager.stream);

        loop {
            match self.retries(operation) {
                Ok(frame) => {
                    self.unanswered_queries.pop();
                    return Ok(frame);
                }
                Err(_) => {
                    self.reconnect()?;
                    self.retry_pending_query()?;
                }
            }
        }
    }
}

impl ClientManager {
    fn execute_query(&mut self, query: &Frame) -> io::Result<Frame> {
        self.write(query)?;
        self.read()
    }

    fn retry_pending_query(&mut self) -> io::Result<()> {
        match self.unanswered_queries.pop() {
            Some(frame) => self.write(&frame),
            None => Ok(()),
        }
    }

    /// Handles retries for a given operation.
    fn retries<F, T>(&mut self, mut operation: F) -> io::Result<T>
    where
        F: FnMut(&mut Self) -> io::Result<T>,
    {
        let mut attempts = 0;

        while attempts < RETRIES {
            match operation(self) {
                Ok(result) => return Ok(result),
                Err(_) => {
                    attempts += 1;
                    eprintln!(
                        "({}) Attempt {} failed, retrying...",
                        self.stream_id, attempts
                    );
                }
            }
        }

        Err(io::Error::new(
            io::ErrorKind::Other,
            format!("Failed after {} attempts", RETRIES),
        ))
    }

    /// Attempts to reconnect the client manager to a server.
    fn reconnect(&mut self) -> io::Result<()> {
        eprintln!("Failed after {} attempts, reconnecting...", RETRIES);

        let mock_manager = ClientManager::new(&self.addresses)?;

        self.stream = mock_manager.stream;
        self.stream_id = mock_manager.stream_id;
        self.encryption_handler = mock_manager.encryption_handler;

        let current_keyspace = self.current_keyspace.clone();
        self.use_keyspace(&current_keyspace)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }
}

/// Connects to the first available address from the given list.
fn connect_to_first_available(addresses: &[String]) -> io::Result<TcpStream> {
    let mut shuffle = addresses.to_vec();
    shuffle.shuffle(&mut rng());

    for address in &shuffle {
        match TcpStream::connect(address) {
            Ok(stream) => return Ok(stream),
            Err(e) => eprintln!("Failed to connect to {}: {}", address, e),
        }
    }

    Err(io::Error::new(
        io::ErrorKind::ConnectionRefused,
        format!(
            "Could not connect to any of the specified addresses: {:?}",
            addresses
        ),
    ))
}
