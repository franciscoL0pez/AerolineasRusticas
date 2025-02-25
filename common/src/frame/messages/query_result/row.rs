use crate::frame::messages::notation::{read_bytes, write_bytes};
use std::io;
use std::io::Cursor;

#[derive(Debug, PartialEq, Clone)]
pub struct Row {
    pub values: Vec<Vec<u8>>, // vector de bytes (Vec<u8>)
}

impl Row {
    pub fn deserialize(columns_count: usize, cursor: &mut Cursor<&[u8]>) -> io::Result<Self> {
        let mut values = Vec::with_capacity(columns_count);
        for _ in 0..columns_count {
            let value = read_bytes(cursor)?;
            values.push(value);
        }
        //println!("values: {:?}", values);
        Ok(Row { values })
    }

    pub fn serialize(&self, buffer: &mut Vec<u8>) {
        for value in &self.values {
            write_bytes(buffer, value);
        }
    }
}

#[cfg(test)]
mod tests {
    //use chrono::naive::serde::ts_microseconds::deserialize;

    use super::*;
    /*
    fn dummy(values: &[&str]) -> Vec<u8> {
        let mut vec: Vec<u8> = Vec::new();
        for value in values {
            vec.extend_from_slice(value.as_bytes());
        }
        vec
    }*/

    #[test]
    fn test_deserialize() {
        let mut data = Vec::new();
        write_bytes(&mut data, b"val1".as_slice());
        write_bytes(&mut data, b"val2".as_slice());
        let row = Row::deserialize(2, &mut Cursor::new(&data)).unwrap();

        assert_eq!(row.values.len(), 2);
        assert_eq!(row.values[0], b"val1".to_vec());
        assert_eq!(row.values[1], b"val2".to_vec());
    }

    #[test]
    fn test_serialize() {
        let row = Row {
            values: vec![b"val1".to_vec(), b"val2".to_vec()],
        };

        let mut buffer = Vec::new();
        row.serialize(&mut buffer);

        let expected = vec![
            0, 0, 0, 4, // Length prefix for "val1"
            b'v', b'a', b'l', b'1', // The bytes for "val1"
            0, 0, 0, 4, // Length prefix for "val2"
            b'v', b'a', b'l', b'2', // The bytes for "val2"
        ];

        assert_eq!(buffer, expected);
    }

    #[test]
    fn test_empty_row() {
        let row = Row { values: vec![] };

        let mut buffer = Vec::new();
        row.serialize(&mut buffer);

        assert_eq!(buffer.len(), 0); // Serialize should not write anything for an empty row
    }
}
