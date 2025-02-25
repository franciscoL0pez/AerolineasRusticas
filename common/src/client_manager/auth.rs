use std::{io::{self, Write}, net::TcpStream};

use crate::{frame::{messages::authentication::{AuthChallenge, AuthResponse}, Frame}, security::EncryptionHandler};

/// Initializes a new `EncryptionHandler` with the given parameters and returns a tuple containing:
/// - `encryption_handler`: An instance of `EncryptionHandler` initialized with the provided parameters.
/// - `public_key`: The public key generated during the initialization.
/// - `shared_secret`: The shared secret generated during the initialization.
///
/// # Parameters
/// - `challenge.prime`: The prime number used for encryption.
/// - `challenge.base`: The base number used for encryption.
/// - `challenge.public_key`: The public key provided for the challenge.
///
/// # Returns
/// A tuple containing the initialized `EncryptionHandler`, the generated public key, and the shared secret.

pub fn authenticate_to_server(stream: &mut TcpStream) -> io::Result<(EncryptionHandler, i16)> {
    let startup = Frame::new_startup();
    stream.write_all(&startup.serialize())?;

    let server_response = read_non_encrypted_frame(stream)?;
    let (authentication, stream_id) = server_response.get_authenticator()?;
    if authentication != "PLAIN" {
        return Err(io::Error::new(
            io::ErrorKind::ConnectionRefused,
            "Unsupported authentication method",
        ));
    }

    let auth_response = server_response.new_auth_response(vec![]);
    stream.write_all(&auth_response.serialize())?;

    let server_response = read_non_encrypted_frame(stream)?;
    let challenge = AuthChallenge::deserialize(&server_response.get_auth_challenge()?);

    let (encryption_handler, public_key, shared_secret) =
        EncryptionHandler::new_initialized(challenge.prime, challenge.base, challenge.public_key);

    let auth_response = AuthResponse::new(public_key, shared_secret);
    let auth_response = server_response.new_auth_response(auth_response.serialize());
    stream.write_all(&auth_response.serialize())?;

    /* Esto es medio choto, pero una vez que el native protocol handler del nodo

    determina que el shared secret es el mismo, ya encripta los mensajes,

    incluyendo el de auth success, por lo que habia un bug si no leia el auth success con el decryptor */

    let response = encryption_handler.read(stream)?;
    match response.is_success() {
        true => Ok((encryption_handler, stream_id)),
        false => Err(io::Error::new(
            io::ErrorKind::ConnectionRefused,
            "Authentication failed",
        )),
    }
}

fn read_non_encrypted_frame(stream: &mut TcpStream) -> io::Result<Frame> {
    Frame::deserialize_from_stream(stream, &|bytes| bytes.to_vec())
}