use std::{io::Read, io::Write, net::TcpStream};

#[derive(Debug, Clone,PartialEq)]
/// Enum representing the different types of messages that can be sent between nodes
/// 
/// The protocol is as follows:
/// 
/// 1. The first byte is the message type:
///    - 0: Gossip message
///   - 1: Query message
///  - 2: Response message
/// 2. The second byte is the opcode of the message
/// 3. The next 4 bytes are the length of the body of the message
/// 4. The next n bytes are the body of the message
/// 5. If the message is a Query message, the next byte is the length of the keyspace name
/// 6. The next n bytes are the keyspace name
/// 7. If the message is a Query message, the next byte is the length of the consistency level
/// 8. The next n bytes are the consistency level
/// 
pub enum InternalMessage {
    /// Gossip message (0): GOSSIP, NEW_NODE
    Gossip {
        /// 0: GOSSIP, 1: NEW_NODE
        opcode: u8,
        body: String,
    },
    /// Query message (1): CREATE_KEYSPACE, CREATE_TABLE, INSERT, SELECT, UPDATE, DELETE
    Query {
        /// 0: CREATE_KEYSPACE, 1: CREATE_TABLE, 2: INSERT, 3: SELECT, 4: UPDATE, 5: DELETE
        opcode: u8,
        body: String,
        keyspace_name: String,
    },
    /// Response message (2): OK, ERROR
    Response {
        /// 0: OK, 1: ERROR
        opcode: u8,
        body: String,
    },
}

impl InternalMessage {
    /// Serialize the message to a byte vector and write it to a TcpStream according to the protocol.
    /// 
    /// # Arguments
    /// 
    /// * `stream` - The TcpStream to write the message to
    /// 
    /// # Returns
    /// 
    /// An empty Result Ok if the message was successfully written to the stream, or an error message if it failed
    /// 
    pub fn write_to_stream(&self, stream: &mut TcpStream) -> Result<(), String> {
        let mut buffer = vec![];
        match self {
            InternalMessage::Gossip { opcode, body } => {
                buffer.push(0);
                buffer.push(*opcode);
                let body_len: u32 = body.len() as u32;
                buffer.extend_from_slice(&body_len.to_be_bytes());
                buffer.extend_from_slice(body.as_bytes());

                if let Err(e) = stream.write_all(&buffer) {
                    return Err(format!("Error writing to stream: {}", e));
                }
            }
            InternalMessage::Query {
                opcode,
                body,
                keyspace_name,
            } => {
                buffer.push(1);
                buffer.push(*opcode);
                let body_len: u32 = body.len() as u32;
                buffer.extend_from_slice(&body_len.to_be_bytes());
                buffer.extend_from_slice(body.as_bytes());
                buffer.push(keyspace_name.len() as u8);
                buffer.extend_from_slice(keyspace_name.as_bytes());

                if let Err(e) = stream.write_all(&buffer) {
                    return Err(format!("Error writing to stream: {}", e));
                }
            }
            InternalMessage::Response { opcode, body } => {
                buffer.push(2);
                buffer.push(*opcode);
                let body_len: u32 = body.len() as u32;
                buffer.extend_from_slice(&body_len.to_be_bytes());
                buffer.extend_from_slice(body.as_bytes());

                if let Err(e) = stream.write_all(&buffer) {
                    return Err(format!("Error writing to stream: {}", e));
                }
            }
        }

        Ok(())
    }

    /// Deserialize a message from a TcpStream according to the protocol.
    /// 
    /// # Arguments
    /// 
    /// * `stream` - The TcpStream to read the message from
    /// 
    /// # Returns
    /// 
    /// An InternalMessage if the message was successfully read from the stream, or an error message if it failed
    /// 
    pub fn deserialize_from_stream(stream: &mut TcpStream) -> Result<Self, String> {
        let mut message_type = [0u8; 1];
        let mut opcode = [0u8; 1];
        let mut body_length = [0u8; 4];
        stream
            .read_exact(&mut message_type)
            .map_err(|e| e.to_string())?;
        stream.read_exact(&mut opcode).map_err(|e| e.to_string())?;
        stream
            .read_exact(&mut body_length)
            .map_err(|e| e.to_string())?;
        let mut body = vec![0u8; u32::from_be_bytes(body_length) as usize];
        stream.read_exact(&mut body).map_err(|e| e.to_string())?;
        let body_as_string = match String::from_utf8(body.clone()) {
            Ok(body_as_string) => body_as_string,
            Err(e) => return Err(e.to_string()),
        };
        match message_type[0] {
            0 => Ok(InternalMessage::Gossip {
                opcode: opcode[0],
                body: body_as_string,
            }),
            1 => {
                let mut keyspace_name_length = [0u8; 1];
                stream
                    .read_exact(&mut keyspace_name_length)
                    .map_err(|e| e.to_string())?;
                let mut keyspace_name = vec![0u8; keyspace_name_length[0] as usize];
                stream
                    .read_exact(&mut keyspace_name)
                    .map_err(|e| e.to_string())?;
                Ok(InternalMessage::Query {
                    opcode: opcode[0],
                    body: body_as_string,
                    keyspace_name: String::from_utf8(keyspace_name).map_err(|e| e.to_string())?,
                })
            }
            2 => Ok(InternalMessage::Response {
                opcode: opcode[0],
                body: body_as_string,
            }),
            _ => Err("Invalid message type".to_string()),
        }
    }
}
