use crate::frame::messages::notation::{
    read_short, read_string, write_short, write_string,
};
use crate::frame::messages::query_result::metadata::option;
use std::io;
use std::io::Cursor;

#[derive(Debug, Clone, Default)]
pub(crate) struct UDTSpec {
    keyspace_name: String,
    udt_name: String,
    fields: Vec<(String, option::Option)>, // (field name, field type)
}

impl UDTSpec {
    pub(crate) fn read_udt(cursor: &mut Cursor<&[u8]>) -> io::Result<Self> {
        let keyspace_name = read_string(cursor)?;
        let udt_name = read_string(cursor)?;
        let n = read_short(cursor)? as usize;
        let mut fields = Vec::with_capacity(n);
        for _ in 0..n {
            let name_i = read_string(cursor)?;
            let type_i = option::Option::read_option(cursor)?;
            fields.push((name_i, type_i));
        }

        Ok(UDTSpec {
            keyspace_name,
            udt_name,
            fields,
        })
    }

    pub fn write(&self, buffer: &mut Vec<u8>) {
        write_string(buffer, &self.keyspace_name);
        write_string(buffer, &self.udt_name);

        write_short(buffer, self.fields.len() as u16);

        for (field_name, field_type) in &self.fields {
            write_string(buffer, field_name);
            field_type.write(buffer)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::messages::notation::write_int;
    use std::io::Cursor;

    fn create_test_udt() -> UDTSpec {
        UDTSpec {
            keyspace_name: "test_keyspace".to_string(),
            udt_name: "test_udt".to_string(),
            fields: vec![
                ("field1".to_string(), option::Option::Ascii),
                ("field2".to_string(), option::Option::Int),
            ],
        }
    }

    fn serialize_test_udt() -> Vec<u8> {
        let mut buffer = Vec::new();

        write_string(&mut buffer, "test_keyspace"); // keyspace_name
        write_string(&mut buffer, "test_udt"); // udt_name
        write_short(&mut buffer, 2); // number of fields

        // field1 -> Ascii type
        write_string(&mut buffer, "field1");
        write_int(&mut buffer, 0x0001); // Ascii is 0x0001

        // field2 -> Int type
        write_string(&mut buffer, "field2");
        write_int(&mut buffer, 0x0009); // Int is 0x0009

        buffer
    }

    #[test]
    fn test_udt_spec_parse() {
        // Arrange
        let data = serialize_test_udt();
        let mut cursor = Cursor::new(data.as_slice());

        // Act
        let parsed_udt = UDTSpec::read_udt(&mut cursor).unwrap();

        // Assert
        assert_eq!(parsed_udt.keyspace_name, "test_keyspace");
        assert_eq!(parsed_udt.udt_name, "test_udt");
        assert_eq!(parsed_udt.fields.len(), 2);
        assert_eq!(parsed_udt.fields[0].0, "field1");
        assert!(matches!(parsed_udt.fields[0].1, option::Option::Ascii));
        assert_eq!(parsed_udt.fields[1].0, "field2");
        assert!(matches!(parsed_udt.fields[1].1, option::Option::Int));
    }

    #[test]
    fn test_udt_spec_write() {
        // Arrange
        let test_udt = create_test_udt();
        let expected_bytes = serialize_test_udt();

        // Act
        let mut buffer = Vec::new();
        test_udt.write(&mut buffer);

        // Assert
        assert_eq!(buffer, expected_bytes);
    }
}
