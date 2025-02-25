use crate::frame::messages::error::ErrorCode;
use crate::frame::messages::Message;
use crate::frame::version::Version;
use std::io;
use std::io::Read;
use std::net::TcpStream;

mod version;
mod client_handle;
pub mod server_handle;
pub mod messages;

const HEADER_SIZE: usize = 9; // 9 BYTES
const MAX_FRAME_SIZE: usize = 256 * 1024 * 1024; // 256 MB

#[derive(Debug, Clone)]
pub struct Frame {
    version: Version,
    compression: bool,
    tracing: bool,
    stream: i16,
    body: Message,
}

/// Sección (des)serializacion
/// Represents a frame in the protocol, which can be serialized and deserialized.
///
/// # Methods
///
/// - `deserialize_from_stream`: Deserializes a `Frame` from a given `TcpStream` using a provided decryptor function.
/// - `serialize`: Serializes the `Frame` into a vector of bytes.
/// - `new_error`: Creates a new `Frame` representing an error with a given error code and stream ID.
/// - `new_protocol_error`: Creates a new `Frame` representing a protocol error with a given stream ID.
///
/// # Fields
///
/// - `version`: The version of the protocol.
/// - `compression`: A flag indicating whether compression is enabled.
/// - `tracing`: A flag indicating whether tracing is enabled.
/// - `stream`: The stream ID associated with the frame.
/// - `body`: The message body of the frame.
impl Frame {
    pub fn deserialize_from_stream(
        stream: &mut TcpStream,
        decryptor: &dyn Fn(&[u8]) -> Vec<u8>,
    ) -> io::Result<Self> {
        let mut encrypted_header = [0u8; HEADER_SIZE];
        stream.read_exact(&mut encrypted_header)?;
        let header = decryptor(&encrypted_header);

        let stream_id = i16::from_be_bytes([header[2], header[3]]);

        let Ok(version) = Version::try_from(header[0]) else {
            return Ok(Frame::new_protocol_error(stream_id));
        };

        let length = u32::from_be_bytes([header[5], header[6], header[7], header[8]]);
        if length > (MAX_FRAME_SIZE - HEADER_SIZE) as u32 {
            return Ok(Self::new_protocol_error(stream_id));
        }

        let mut encrypted_body = vec![0u8; length as usize];
        stream.read_exact(&mut encrypted_body)?;
        let body = decryptor(&encrypted_body);

        let op_code = header[4];
        let Ok(body) = Message::deserialize(op_code, body) else {
            return Ok(Self::new_protocol_error(stream_id));
        };

        let flags = header[1];
        let (compression, tracing) = match flags {
            0x01 => (true, false),
            0x02 => (false, true),
            0x03 => (true, false),
            _ => (false, false),
        };

        Ok(Frame {
            version,
            compression,
            tracing,
            stream: stream_id,
            body,
        })
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes: Vec<u8> = Vec::new();

        bytes.push(u8::from(self.version));

        let flags: u8 = match (self.compression, self.tracing) {
            (true, false) => 0x01,
            (false, true) => 0x02,
            (true, true) => 0x03,
            (false, false) => 0x00,
        };
        bytes.push(flags);

        bytes.extend_from_slice(&self.stream.to_be_bytes());
        bytes.push(self.body.to_op_code());

        let body_bytes = self.body.serialize();
        let length = body_bytes.len() as i32;
        bytes.extend_from_slice(&length.to_be_bytes());
        bytes.extend_from_slice(&body_bytes);

        bytes
    }

    fn new_error(code: ErrorCode, stream: i16) -> Self {
        Frame {
            version: Version::ResponseV3,
            compression: false,
            tracing: false,
            stream,
            body: Message::Error(code),
        }
    }

    pub fn new_protocol_error(stream: i16) -> Self {
        Frame::new_error(ErrorCode::ProtocolError, stream)
    }
}
