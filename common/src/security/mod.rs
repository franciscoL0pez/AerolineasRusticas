pub mod base_encryption_functions;

use base_encryption_functions::{decrypt, encrypt};
use rand::{rng, Rng};
use std::{
    io::{self, Write},
    net::TcpStream,
};

use crate::frame::Frame;

// codigo ultra secreto

#[derive(Debug)]
pub struct EncryptionHandler {
    prime: u64,
    base: u64,
    public_key: u64,
    private_key: u64,
    shared_secret: Option<u64>,
}

impl EncryptionHandler {
    pub fn new(prime: u64, base: u64) -> Self {
        let private_key = generate_private_key();
        let my_public_key = generate_public_key(private_key, base, prime);

        Self {
            prime,
            base,
            public_key: my_public_key,
            private_key,
            shared_secret: None,
        }
    }

    pub fn new_initialized(prime: u64, base: u64, other_public_key: u64) -> (Self, u64, u64) {
        let private_key = generate_private_key();
        let my_public_key = generate_public_key(private_key, base, prime);
        let shared_secret = generate_shared_secret(other_public_key, private_key, prime);

        (
            Self {
                prime,
                base,
                public_key: my_public_key,
                private_key,
                shared_secret: Some(shared_secret),
            },
            my_public_key,
            shared_secret,
        )
    }

    pub fn attempt_initialize(
        &mut self,
        other_public_key: u64,
        challenged_shared_secret: u64,
    ) -> bool {
        let shared_secret = generate_shared_secret(other_public_key, self.private_key, self.prime);

        if shared_secret == challenged_shared_secret {
            self.shared_secret = Some(shared_secret);
            return true;
        }

        false
    }

    pub fn read(&self, stream: &mut TcpStream) -> io::Result<Frame> {
        Frame::deserialize_from_stream(stream, &self.get_decryptor())
    }

    pub fn write(&self, stream: &mut TcpStream, frame: &Frame) -> io::Result<()> {
        let bytes = frame.serialize();
        let bytes = match self.shared_secret {
            Some(shared_secret) => encrypt(&bytes, shared_secret),
            None => bytes,
        };
        stream.write_all(&bytes)
    }

    #[allow(clippy::type_complexity)]
    fn get_decryptor(&self) -> Box<dyn Fn(&[u8]) -> Vec<u8>> {
        match self.shared_secret {
            Some(shared_secret) => Box::new(move |data| decrypt(data, shared_secret)),
            None => Box::new(|bytes: &[u8]| bytes.to_vec()),
        }
    }

    pub fn get_dh_params(&self) -> (u64, u64, u64) {
        (self.public_key, self.prime, self.base)
    }
}

fn generate_private_key() -> u64 {
    let mut rng = rng();
    rng.random_range(1..100_000)
}

fn generate_public_key(private_key: u64, g: u64, p: u64) -> u64 {
    mod_exp(g, private_key, p)
}

fn generate_shared_secret(public_key: u64, private_key: u64, p: u64) -> u64 {
    mod_exp(public_key, private_key, p)
}

fn mod_exp(mut base: u64, mut exp: u64, modulus: u64) -> u64 {
    let mut result = 1;
    base %= modulus;

    while exp > 0 {
        if exp % 2 == 1 {
            result = (result * base) % modulus;
        }
        exp >>= 1;
        base = (base * base) % modulus;
    }
    result
}
