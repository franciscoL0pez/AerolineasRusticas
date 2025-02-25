use crate::frame::messages::notation::{read_string, write_string};
use crate::frame::messages::query_result::metadata::option::Option;
use std::io;
use std::io::Cursor;

#[derive(Debug, Clone)]
pub struct NotGlobalCol {
    keyspace_name: String,
    table_name: String,
    name: String,
    option: Option,
}

impl NotGlobalCol {
    fn parse(cursor: &mut Cursor<&[u8]>) -> io::Result<Self> {
        let keyspace_name = read_string(cursor)?;
        let table_name = read_string(cursor)?;
        let name = read_string(cursor)?;
        let option = Option::read_option(cursor)?;
        Ok(NotGlobalCol {
            keyspace_name,
            table_name,
            name,
            option,
        })
    }

    fn write(&self, buffer: &mut Vec<u8>) {
        write_string(buffer, &self.keyspace_name);
        write_string(buffer, &self.table_name);
        write_string(buffer, &self.name);
        self.option.write(buffer);
    }
}

#[derive(Debug, Clone)]
pub struct GlobalSpec {
    keyspace_name: String,
    table_name: String,
    cols: Vec<(String, Option)>, // Name, OptionType
}

impl GlobalSpec {
    fn deserialize(cursor: &mut Cursor<&[u8]>, column_count: usize) -> io::Result<Self> {
        let keyspace_name = read_string(cursor)?;
        let table_name = read_string(cursor)?;

        let mut cols = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            let col_name = read_string(cursor)?;
            let option = Option::read_option(cursor)?;
            cols.push((col_name, option));
        }

        Ok(GlobalSpec {
            keyspace_name,
            table_name,
            cols,
        })
    }

    fn serialize(&self, buffer: &mut Vec<u8>) {
        write_string(buffer, &self.keyspace_name);
        write_string(buffer, &self.table_name);
        for (col_name, option) in &self.cols {
            write_string(buffer, col_name);
            option.write(buffer);
        }
    }
}

#[derive(Debug, Clone)]
pub enum Spec {
    Global(GlobalSpec),
    NotGlobal(Vec<NotGlobalCol>),
}

impl Spec {
    pub(crate) fn deserialize(
        cursor: &mut Cursor<&[u8]>,
        global: bool,
        columns_count: usize,
    ) -> io::Result<Self> {
        if global {
            return Ok(Self::Global(GlobalSpec::deserialize(
                cursor,
                columns_count,
            )?));
        }

        let mut columns = Vec::with_capacity(columns_count);
        for _ in 0..columns_count {
            columns.push(NotGlobalCol::parse(cursor)?);
        }
        Ok(Self::NotGlobal(columns))
    }

    pub(crate) fn serialize(&self, buffer: &mut Vec<u8>) {
        match self {
            Spec::Global(ref global_spec) => global_spec.serialize(buffer),
            Spec::NotGlobal(ref not_global_cols) => {
                for col in not_global_cols {
                    col.write(buffer)
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
    fn test_not_global_col_parse_and_write() {
        let input_data = vec![
            0, 3, b'a', b'b', b'c', // keyspace_name: "abc"
            0, 4, b't', b'e', b's', b't', // table_name: "test"
            0, 5, b'f', b'i', b'e', b'l', b'd', // name: "field"
            0, 0, 0, 1, // Option::Ascii
        ];

        let mut cursor = Cursor::new(input_data.as_slice());
        let not_global_col = NotGlobalCol::parse(&mut cursor).unwrap();

        // Assert parsed values
        assert_eq!(not_global_col.keyspace_name, "abc");
        assert_eq!(not_global_col.table_name, "test");
        assert_eq!(not_global_col.name, "field");
        assert!(matches!(not_global_col.option, Option::Ascii));

        // Write back to buffer
        let mut output_data = Vec::new();
        not_global_col.write(&mut output_data);

        // Ensure the written data matches the input data
        assert_eq!(output_data, input_data);
    }

    #[test]
    fn test_global_spec_parse_and_write() {
        let input_data = vec![
            0, 3, b'a', b'b', b'c', // keyspace_name: "abc"
            0, 4, b't', b'e', b's', b't', // table_name: "test"
            // First column
            0, 4, b'c', b'o', b'l', b'1', // column name: "col1"
            0, 0, 0, 1, // Option::Ascii
            // Second column
            0, 4, b'c', b'o', b'l', b'2', // column name: "col2"
            0, 0, 0, 9, // Option::Int
        ];

        let mut cursor = Cursor::new(input_data.as_slice());
        let global_spec = GlobalSpec::deserialize(&mut cursor, 2).unwrap();

        // Assert parsed values
        assert_eq!(global_spec.keyspace_name, "abc");
        assert_eq!(global_spec.table_name, "test");
        assert_eq!(global_spec.cols.len(), 2);
        assert_eq!(global_spec.cols[0].0, "col1");
        assert!(matches!(global_spec.cols[0].1, Option::Ascii));
        assert_eq!(global_spec.cols[1].0, "col2");
        assert!(matches!(global_spec.cols[1].1, Option::Int));

        // Write back to buffer
        let mut output_data = Vec::new();
        global_spec.serialize(&mut output_data);

        // Ensure the written data matches the input data
        assert_eq!(output_data, input_data);
    }

    #[test]
    fn test_spec_global_parse_and_write() {
        let input_data = vec![
            0, 3, b'a', b'b', b'c', // keyspace_name: "abc"
            0, 4, b't', b'e', b's', b't', // table_name: "test"
            // Column
            0, 4, b'c', b'o', b'l', b'1', // column name: "col1"
            0, 0, 0, 1, // Option::Ascii
        ];

        let mut cursor = Cursor::new(input_data.as_slice());
        let spec = Spec::deserialize(&mut cursor, true, 1).unwrap();

        // Assert parsed values
        if let Spec::Global(ref global_spec) = spec {
            assert_eq!(global_spec.keyspace_name, "abc");
            assert_eq!(global_spec.table_name, "test");
            assert_eq!(global_spec.cols.len(), 1);
            assert_eq!(global_spec.cols[0].0, "col1");
            assert!(matches!(global_spec.cols[0].1, Option::Ascii));
        } else {
            panic!("Expected Spec::Global");
        }

        // Write back to buffer
        let mut output_data = Vec::new();
        spec.serialize(&mut output_data);

        // Ensure the written data matches the input data
        assert_eq!(output_data, input_data);
    }

    #[test]
    fn test_spec_not_global_parse_and_write() {
        let input_data = vec![
            0, 3, b'a', b'b', b'c', // keyspace_name: "abc"
            0, 4, b't', b'e', b's', b't', // table_name: "test"
            0, 5, b'f', b'i', b'e', b'l', b'd', // column name: "field"
            0, 0, 0, 9, // Option::Int
        ];

        let mut cursor = Cursor::new(input_data.as_slice());
        let spec = Spec::deserialize(&mut cursor, false, 1).unwrap();

        // Assert parsed values
        if let Spec::NotGlobal(ref cols) = spec {
            assert_eq!(cols.len(), 1);
            assert_eq!(cols[0].keyspace_name, "abc");
            assert_eq!(cols[0].table_name, "test");
            assert_eq!(cols[0].name, "field");
            assert!(matches!(cols[0].option, Option::Int));
        } else {
            panic!("Expected Spec::NotGlobal");
        }

        // Write back to buffer
        let mut output_data = Vec::new();
        spec.serialize(&mut output_data);

        // Ensure the written data matches the input data
        assert_eq!(output_data, input_data);
    }
}
