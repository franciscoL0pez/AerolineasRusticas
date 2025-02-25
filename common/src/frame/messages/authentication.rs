use super::notation::{read_string, write_string};
use std::io::Cursor;

pub fn serialize_authenticate(iauthenticator: &str) -> Vec<u8> {
    let mut buffer = vec![];
    write_string(&mut buffer, iauthenticator);
    buffer
}

pub fn deserialize_authenticate(buffer: &[u8]) -> String {
    let mut cursor = Cursor::new(buffer);
    read_string(&mut cursor).unwrap()
}

#[derive(Debug)]
pub struct AuthChallenge {
    pub public_key: u64,
    pub prime: u64,
    pub base: u64,
}

impl AuthChallenge {
    pub fn new(public_key: u64, prime: u64, base: u64) -> Self {
        Self {
            public_key,
            prime,
            base,
        }
    }

    pub fn deserialize(bytes: &[u8]) -> Self {
        let public_key = u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]);

        let prime = u64::from_be_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
        ]);

        let base = u64::from_be_bytes([
            bytes[16], bytes[17], bytes[18], bytes[19], bytes[20], bytes[21], bytes[22], bytes[23],
        ]);

        Self::new(public_key, prime, base)
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.public_key.to_be_bytes());
        bytes.extend_from_slice(&self.prime.to_be_bytes());
        bytes.extend_from_slice(&self.base.to_be_bytes());
        bytes
    }
}

pub struct AuthResponse {
    pub public_key: u64,
    pub shared_secret: u64,
}

impl AuthResponse {
    pub fn new(public_key: u64, shared_secret: u64) -> Self {
        Self {
            public_key,
            shared_secret,
        }
    }

    pub fn deserialize(bytes: &[u8]) -> Self {
        let public_key = u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]);

        let shared_secret = u64::from_be_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
        ]);

        Self::new(public_key, shared_secret)
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.public_key.to_be_bytes());
        bytes.extend_from_slice(&self.shared_secret.to_be_bytes());
        bytes
    }
}