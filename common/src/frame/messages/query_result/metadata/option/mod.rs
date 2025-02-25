use crate::frame::messages::notation::{
    read_int, read_short, read_string, write_int, write_short, write_string,
};
use std::io;
use std::io::Cursor;

mod udt;

#[derive(Debug, Clone)]
#[repr(i32)]
pub(crate) enum Option {
    Custom(String) = 0x0000, // Custom type with fully qualified class name
    Ascii = 0x0001,
    Bigint = 0x0002,
    Blob = 0x0003,
    Boolean = 0x0004,
    Counter = 0x0005,
    Decimal = 0x0006,
    Double = 0x0007,
    Float = 0x0008,
    Int = 0x0009,
    Timestamp = 0x000B,
    Uuid = 0x000C,
    Varchar = 0x000D,
    Varint = 0x000E,
    Timeuuid = 0x000F,
    Inet = 0x0010,
    List(Box<Option>) = 0x0020,             // For list types
    Map(Box<Option>, Box<Option>) = 0x0021, // For map types (key, value)
    Set(Box<Option>) = 0x0022,              // For set types
    Udt(udt::UDTSpec) = 0x0030,             // For Udt types
    Tuple(Vec<Option>) = 0x0031,            // For tuple types
}

impl Option {
    pub(crate) fn read_option(cursor: &mut Cursor<&[u8]>) -> io::Result<Self> {
        let option_value = read_int(cursor)?;
        match option_value {
            0x0000 => {
                let string_value = read_string(cursor)?;
                Ok(Option::Custom(string_value))
            }
            0x0001 => Ok(Option::Ascii),
            0x0002 => Ok(Option::Bigint),
            0x0003 => Ok(Option::Blob),
            0x0004 => Ok(Option::Boolean),
            0x0005 => Ok(Option::Counter),
            0x0006 => Ok(Option::Decimal),
            0x0007 => Ok(Option::Double),
            0x0008 => Ok(Option::Float),
            0x0009 => Ok(Option::Int),
            0x000B => Ok(Option::Timestamp),
            0x000C => Ok(Option::Uuid),
            0x000D => Ok(Option::Varchar),
            0x000E => Ok(Option::Varint),
            0x000F => Ok(Option::Timeuuid),
            0x0010 => Ok(Option::Inet),
            0x0020 => {
                let inner = Box::new(Self::read_option(cursor)?);
                Ok(Option::List(inner))
            }
            0x0021 => {
                let key = Box::new(Self::read_option(cursor)?);
                let value = Box::new(Self::read_option(cursor)?);
                Ok(Option::Map(key, value))
            }
            0x0022 => {
                let inner = Box::new(Self::read_option(cursor)?);
                Ok(Option::Set(inner))
            }
            0x0030 => {
                let udt_spec = udt::UDTSpec::read_udt(cursor)?;
                Ok(Option::Udt(udt_spec))
            }
            0x0031 => {
                let n = read_short(cursor)? as usize;
                let mut types = Vec::with_capacity(n);
                for _ in 0..n {
                    let inner_type = Self::read_option(cursor)?;
                    types.push(inner_type);
                }
                Ok(Option::Tuple(types))
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid Option Type",
            )),
        }
    }

    pub fn write(&self, buffer: &mut Vec<u8>) {
        match self {
            Option::Custom(ref custom_name) => {
                write_int(buffer, 0x00); // Assuming 0x00 is the identifier for Custom
                write_string(buffer, custom_name);
            }
            Option::Ascii => write_int(buffer, 0x01),
            Option::Bigint => write_int(buffer, 0x02),
            Option::Blob => write_int(buffer, 0x03),
            Option::Boolean => write_int(buffer, 0x04),
            Option::Counter => write_int(buffer, 0x05),
            Option::Decimal => write_int(buffer, 0x06),
            Option::Double => write_int(buffer, 0x07),
            Option::Float => write_int(buffer, 0x08),
            Option::Int => write_int(buffer, 0x09),
            Option::Timestamp => write_int(buffer, 0x0B),
            Option::Uuid => write_int(buffer, 0x0C),
            Option::Varchar => write_int(buffer, 0x0D),
            Option::Varint => write_int(buffer, 0x0E),
            Option::Timeuuid => write_int(buffer, 0x0F),
            Option::Inet => write_int(buffer, 0x10),
            Option::List(ref inner) => {
                write_int(buffer, 0x20);
                inner.write(buffer);
            }
            Option::Map(ref key, ref value) => {
                write_int(buffer, 0x21);
                key.write(buffer);
                value.write(buffer);
            }
            Option::Set(ref inner) => {
                write_int(buffer, 0x22);
                inner.write(buffer);
            }
            Option::Udt(ref udt_spec) => {
                write_int(buffer, 0x30);
                udt_spec.write(buffer);
            }
            Option::Tuple(ref types) => {
                write_int(buffer, 0x31);
                write_short(buffer, types.len() as u16); // Write number of types
                for type_option in types {
                    type_option.write(buffer);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_option_custom_write_and_read() {
        let option = Option::Custom("test".to_string());
        let mut buffer = Vec::new();
        option.write(&mut buffer);

        let mut cursor = Cursor::new(buffer.as_slice());
        let read_option = Option::read_option(&mut cursor).unwrap();

        if let Option::Custom(val) = read_option {
            assert_eq!(val, "test");
        } else {
            panic!("Expected Option::Custom, but got {:?}", read_option);
        }
    }

    #[test]
    fn test_option_ascii_write_and_read() {
        let option = Option::Ascii;
        let mut buffer = Vec::new();
        option.write(&mut buffer);

        let mut cursor = Cursor::new(buffer.as_slice());
        let read_option = Option::read_option(&mut cursor).unwrap();

        assert!(matches!(read_option, Option::Ascii));
    }

    #[test]
    fn test_option_list_write_and_read() {
        let option = Option::List(Box::new(Option::Varchar));
        let mut buffer = Vec::new();
        option.write(&mut buffer);

        let mut cursor = Cursor::new(buffer.as_slice());
        let read_option = Option::read_option(&mut cursor).unwrap();

        if let Option::List(inner_option) = read_option {
            assert!(matches!(*inner_option, Option::Varchar));
        } else {
            panic!("Expected Option::List, but got {:?}", read_option);
        }
    }

    #[test]
    fn test_option_map_write_and_read() {
        let option = Option::Map(Box::new(Option::Ascii), Box::new(Option::Bigint));
        let mut buffer = Vec::new();
        option.write(&mut buffer);

        let mut cursor = Cursor::new(buffer.as_slice());
        let read_option = Option::read_option(&mut cursor).unwrap();

        if let Option::Map(key, value) = read_option {
            assert!(matches!(*key, Option::Ascii));
            assert!(matches!(*value, Option::Bigint));
        } else {
            panic!("Expected Option::Map, but got {:?}", read_option);
        }
    }

    #[test]
    fn test_option_tuple_write_and_read() {
        let option = Option::Tuple(vec![Option::Int, Option::Varchar]);
        let mut buffer = Vec::new();
        option.write(&mut buffer);

        let mut cursor = Cursor::new(buffer.as_slice());
        let read_option = Option::read_option(&mut cursor).unwrap();

        if let Option::Tuple(types) = read_option {
            assert_eq!(types.len(), 2);
            assert!(matches!(types[0], Option::Int));
            assert!(matches!(types[1], Option::Varchar));
        } else {
            panic!("Expected Option::Tuple, but got {:?}", read_option);
        }
    }
}
