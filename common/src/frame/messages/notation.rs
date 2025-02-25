use crate::frame::messages::consistency_level::ConsistencyLevel;
use std::io;
use std::io::{Cursor, Read};

/// ```ignore
/// 3. Notations
///
///   To describe the layout of the frame body for the messages in Section 4, we
///   define the following:
///
///     [int]          A 4 bytes signed integer
///     [long]         A 8 bytes signed integer
///     [short]        A 2 bytes unsigned integer
///     [string]       A [short] n, followed by n bytes representing an UTF-8
///                    string.
///     [long string]  An [int] n, followed by n bytes representing an UTF-8 string.
///     [uuid]         A 16 bytes long uuid.
///     [string list]  A [short] n, followed by n [string].
///     [bytes]        A [int] n, followed by n bytes if n >= 0. If n < 0,
///                    no byte should follow and the value represented is `null`.
///     [short bytes]  A [short] n, followed by n bytes if n >= 0.
///
///     [option]       A pair of <id><value> where <id> is a [short] representing
///                    the option id and <value> depends on that option (and can be
///                    of size 0). The supported id (and the corresponding <value>)
///                    will be described when this is used.
///     [option list]  A [short] n, followed by n [option].
///     [inet]         An address (ip and port) to a node. It consists of one
///                    [byte] n, that represents the address size, followed by n
///                    [byte] representing the IP address (in practice n can only be
///                    either 4 (IPv4) or 16 (IPv6)), following by one [int]
///                    representing the port.
///     [consistency]  A consistency level specification. This is a [short]
///                    representing a consistency level with the following
///                    correspondance:
///                      0x0000    ANY
///                      0x0001    ONE
///                      0x0002    TWO
///                      0x0003    THREE
///                      0x0004    QUORUM
///                      0x0005    ALL
///                      0x0006    LOCAL_QUORUM
///                      0x0007    EACH_QUORUM
///                      0x0008    SERIAL
///                      0x0009    LOCAL_SERIAL
///                      0x000A    LOCAL_ONE
///
///     [string map]      A [short] n, followed by n pair <k><v> where <k> and <v>
///                       are [string].
///     [string multimap] A [short] n, followed by n pair <k><v> where <k> is a
///                       [string] and <v> is a [string list].
///```
// Write an [int]
pub fn write_int(buffer: &mut Vec<u8>, value: i32) {
    buffer.extend_from_slice(&value.to_be_bytes());
}

// Write a [long]
pub fn write_long(buffer: &mut Vec<u8>, value: i64) {
    buffer.extend_from_slice(&value.to_be_bytes());
}

// Write a [short]
pub fn write_short(buffer: &mut Vec<u8>, value: u16) {
    buffer.extend_from_slice(&value.to_be_bytes());
}

// Write a [string]
pub fn write_string(buffer: &mut Vec<u8>, value: &str) {
    let length = value.len() as u16;
    write_short(buffer, length); // write [short] n
    buffer.extend_from_slice(value.as_bytes());
}

// Write a [long string]
pub fn write_long_string(buffer: &mut Vec<u8>, value: &str) {
    let length = value.len() as i32;
    write_int(buffer, length); // write [int] n
    buffer.extend_from_slice(value.as_bytes());
}
/*
// Write a [uuid]
pub fn write_uuid(buffer: &mut Vec<u8>, uuid: &[u8; 16]) {
    buffer.extend_from_slice(uuid);
}*/

// Write a [string list]
pub fn write_string_list(buffer: &mut Vec<u8>, strings: Vec<&str>) {
    let n = strings.len() as u16;
    write_short(buffer, n); // write [short] n
    for string in strings {
        write_string(buffer, string);
    }
}

// Write [bytes]
pub fn write_bytes(buffer: &mut Vec<u8>, bytes: &[u8]) {
    write_int(buffer, bytes.len() as i32); // write [int] n
    buffer.extend_from_slice(bytes);
}

// Write [short bytes]
pub fn write_short_bytes(buffer: &mut Vec<u8>, bytes: &[u8]) {
    write_short(buffer, bytes.len() as u16); // write [short] n
    buffer.extend_from_slice(bytes);
}

pub fn write_byte(buffer: &mut Vec<u8>, byte: u8) {
    buffer.push(byte);
}

// Write a [string map]
pub fn write_string_map(buffer: &mut Vec<u8>, kv_pairs: Vec<(&str, &str)>) {
    let n = kv_pairs.len() as u16;
    write_short(buffer, n); // write [short] n
    for (key, value) in kv_pairs {
        write_string(buffer, key);
        write_string(buffer, value);
    }
}

// Write a [string multimap]
pub fn write_string_multimap(buffer: &mut Vec<u8>, kv_pairs: Vec<(&str, Vec<&str>)>) {
    let n = kv_pairs.len() as u16;
    write_short(buffer, n); // write [short] n
    for (key, values) in kv_pairs {
        write_string(buffer, key);
        write_string_list(buffer, values);
    }
}
/*
// Write an [inet] address (IP and port)
pub fn write_inet(buffer: &mut Vec<u8>, ip: &[u8], port: u32) {
    buffer.push(ip.len() as u8); // [byte] n, address size
    buffer.extend_from_slice(ip); // IP address (either 4 or 16 bytes)
    write_int(buffer, port as i32); // port number
}*/

// Write a [consistency]
pub fn write_consistency(buffer: &mut Vec<u8>, consistency_level: ConsistencyLevel) {
    write_short(buffer, consistency_level as u16);
}

pub fn read_short(cursor: &mut Cursor<&[u8]>) -> io::Result<u16> {
    let mut buf = [0; 2];
    cursor.read_exact(&mut buf)?;
    Ok(u16::from_be_bytes(buf))
}

pub fn read_int(cursor: &mut Cursor<&[u8]>) -> io::Result<i32> {
    let mut buf = [0; 4];
    cursor.read_exact(&mut buf)?;
    Ok(i32::from_be_bytes(buf))
}

pub fn read_long(cursor: &mut Cursor<&[u8]>) -> io::Result<i64> {
    let mut buf = [0; 8];
    cursor.read_exact(&mut buf)?;
    Ok(i64::from_be_bytes(buf))
}

pub fn read_string(cursor: &mut Cursor<&[u8]>) -> io::Result<String> {
    let len = read_short(cursor)? as usize;
    let mut buf = vec![0; len];
    cursor.read_exact(&mut buf)?;
    Ok(String::from_utf8(buf).unwrap())
}

pub fn read_long_string(cursor: &mut Cursor<&[u8]>) -> io::Result<String> {
    let len = read_int(cursor)? as usize;
    let mut buf = vec![0; len];
    cursor.read_exact(&mut buf)?;
    Ok(String::from_utf8(buf).unwrap())
}
/*
fn read_uuid(cursor: &mut Cursor<&[u8]>) -> io::Result<[u8; 16]> {
    let mut buf = [0; 16];
    cursor.read_exact(&mut buf)?;
    Ok(buf)
}*/

fn read_string_list(cursor: &mut Cursor<&[u8]>) -> io::Result<Vec<String>> {
    let len = read_short(cursor)?;
    let mut list = Vec::with_capacity(len as usize);
    for _ in 0..len {
        list.push(read_string(cursor)?);
    }
    Ok(list)
}

pub fn read_bytes(cursor: &mut Cursor<&[u8]>) -> io::Result<Vec<u8>> {
    let len = read_int(cursor)?;
    let mut buf = vec![0; len as usize];
    cursor.read_exact(&mut buf)?;
    Ok(buf)
}

pub fn read_short_bytes(cursor: &mut Cursor<&[u8]>) -> io::Result<Vec<u8>> {
    let len = read_short(cursor)? as usize;
    let mut buf = vec![0; len];
    cursor.read_exact(&mut buf)?;
    Ok(buf)
}

pub fn read_byte(cursor: &mut Cursor<&[u8]>) -> io::Result<u8> {
    let mut buf = [0; 1];
    cursor.read_exact(&mut buf)?;
    Ok(buf[0])
}

pub fn read_string_map(cursor: &mut Cursor<&[u8]>) -> io::Result<Vec<(String, String)>> {
    let len = read_short(cursor)?;
    let mut map = Vec::with_capacity(len as usize);
    for _ in 0..len {
        let key = read_string(cursor)?;
        let value = read_string(cursor)?;
        map.push((key, value));
    }
    Ok(map)
}

pub fn read_string_multimap(cursor: &mut Cursor<&[u8]>) -> io::Result<Vec<(String, Vec<String>)>> {
    let len = read_short(cursor)?;
    let mut map = Vec::with_capacity(len as usize);
    for _ in 0..len {
        let key = read_string(cursor)?;
        let value = read_string_list(cursor)?;
        map.push((key, value));
    }
    Ok(map)
}
/*
fn read_inet(cursor: &mut Cursor<&[u8]>) -> io::Result<(Vec<u8>, i32)> {
    let addr_size = read_byte(cursor)? as usize;
    let mut ip = vec![0; addr_size];
    cursor.read_exact(&mut ip)?;
    let port = read_int(cursor)?;
    Ok((ip, port))
}*/

pub fn read_consistency(cursor: &mut Cursor<&[u8]>) -> io::Result<ConsistencyLevel> {
    let consistency = read_short(cursor)?;
    Ok(ConsistencyLevel::from_value(consistency))
}
