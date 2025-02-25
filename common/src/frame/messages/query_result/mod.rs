mod metadata;
pub(crate) mod row;

use crate::frame::messages::notation::{
    read_int, read_short_bytes, read_string, write_int, write_short_bytes, write_string,
};
use metadata::Metadata;
use row::Row;
use std::io::Cursor;
use std::{io, vec};

#[repr(i32)]
#[derive(Debug, Clone)]
pub enum QueryResult {
    Void = 0x0001,
    Rows(Metadata, Vec<Row>) = 0x0002,
    SetKeyspace(String) = 0x0003, // string: keyspace name
    Prepared {
        id: Vec<u8>,
        metadata: Metadata,
        result_metadata: Metadata,
    } = 0x0004,
    SchemaChange {
        change_type: String,
        target: String,
        options: String,
    } = 0x0005,
}

impl QueryResult {
    pub(crate) fn deserialize(body: &[u8]) -> io::Result<Self> {
        let mut cursor = Cursor::new(body);
        let kind = read_int(&mut cursor)?;
        match kind {
            0x0001 => Ok(Self::Void),
            0x0002 => Self::deserialize_rows(&mut cursor),
            0x0003 => {
                let keyspace_name = read_string(&mut cursor)?;
                Ok(Self::SetKeyspace(keyspace_name))
            }
            0x0004 => {
                let id = read_short_bytes(&mut cursor)?;
                let metadata = Metadata::deserialize(&mut cursor)?;
                let result_metadata = Metadata::deserialize(&mut cursor)?;
                Ok(Self::Prepared {
                    id,
                    metadata,
                    result_metadata,
                })
            }
            0x0005 => {
                let change_type = read_string(&mut cursor)?;
                let target = read_string(&mut cursor)?;
                let options = read_string(&mut cursor)?;
                Ok(Self::SchemaChange {
                    change_type,
                    target,
                    options,
                })
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid Result Kind",
            )),
        }
    }

    fn deserialize_rows(cursor: &mut Cursor<&[u8]>) -> io::Result<Self> {
        let metadata = Metadata::deserialize(cursor)?;
        let rows_count = read_int(cursor)? as usize;

        let mut rows = Vec::with_capacity(rows_count);
        let columns_count = metadata.get_columns_count() as usize;
        for _ in 0..rows_count {
            let row_i = Row::deserialize(columns_count, cursor)?;
            rows.push(row_i);
        }
        Ok(Self::Rows(metadata, rows))
    }

    pub(crate) fn serialize(&self) -> Vec<u8> {
        let mut bytes: Vec<u8> = Vec::new();
        match self {
            QueryResult::Void => {
                write_int(&mut bytes, 0x0001);
            }
            QueryResult::Rows(metadata, rows) => {
                write_int(&mut bytes, 0x0002);
                metadata.serialize(&mut bytes);
                write_int(&mut bytes, rows.len() as i32);
                for row in rows {
                    row.serialize(&mut bytes);
                }
            }
            QueryResult::SetKeyspace(name) => {
                write_int(&mut bytes, 0x0003);
                write_string(&mut bytes, name);
            }
            QueryResult::Prepared {
                id,
                metadata,
                result_metadata,
            } => {
                write_int(&mut bytes, 0x0004);
                write_short_bytes(&mut bytes, id);
                metadata.serialize(&mut bytes);
                result_metadata.serialize(&mut bytes);
            }
            QueryResult::SchemaChange {
                change_type,
                target,
                options,
            } => {
                write_int(&mut bytes, 0x0005);
                write_string(&mut bytes, change_type);
                write_string(&mut bytes, target);
                write_string(&mut bytes, options);
            }
        };
        bytes
    }

    pub fn parse_json_to_rows(json: &str) -> QueryResult {
        let serialized_json = Vec::from(json.as_bytes());
        let row = Row {
            values: vec![serialized_json],
        };
        QueryResult::Rows(Metadata::default(), vec![row])
    }

    #[allow(clippy::inherent_to_string)]
    pub fn to_string(&self) -> String {
        match self {
            QueryResult::Void => String::new(),
            QueryResult::Rows(_metadata, rows) => {
                let row = &rows[0];
                let json = String::from_utf8_lossy(&row.values[0]);
                json.to_string()
            }
            QueryResult::SetKeyspace(name) => name.to_owned(),
            QueryResult::Prepared { .. } => String::new(),
            QueryResult::SchemaChange { change_type, .. } => change_type.to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::messages::notation::write_bytes;

    fn row(values: &[&str]) -> io::Result<Row> {
        let mut vec: Vec<u8> = Vec::new();
        for value in values {
            write_bytes(&mut vec, value.as_bytes());
        }
        Row::deserialize(values.len(), &mut Cursor::new(&vec))
    }

    fn metadata() -> Metadata {
        let mut bytes = vec![];
        write_int(&mut bytes, 0x04);
        write_int(&mut bytes, 0x02);
        Metadata::deserialize(&mut Cursor::new(&bytes)).unwrap()
    }

    #[test]
    fn test_row_serialize_and_deserialize() {
        let row = row(&["val1", "val2"]).unwrap();
        let mut buffer = Vec::new();
        row.serialize(&mut buffer);

        let mut cursor = Cursor::new(buffer.as_slice());
        let deserialized_row = Row::deserialize(2, &mut cursor).unwrap();

        assert_eq!(row, deserialized_row);
    }

    #[test]
    fn test_queryresult_void_serialize_and_deserialize() {
        let query_result = QueryResult::Void;
        let serialized = query_result.serialize();
        let deserialized = QueryResult::deserialize(&serialized).unwrap();

        assert!(matches!(deserialized, QueryResult::Void));
    }

    #[test]
    fn test_queryresult_set_keyspace_serialize_and_deserialize() {
        let query_result = QueryResult::SetKeyspace("test_keyspace".to_string());
        let serialized = query_result.serialize();
        let deserialized = QueryResult::deserialize(&serialized).unwrap();

        if let QueryResult::SetKeyspace(keyspace) = deserialized {
            assert_eq!(keyspace, "test_keyspace");
        } else {
            panic!("Expected QueryResult::SetKeyspace");
        }
    }

    #[test]
    fn test_queryresult_schema_change_serialize_and_deserialize() {
        let query_result = QueryResult::SchemaChange {
            change_type: "CREATED".to_string(),
            target: "TABLE".to_string(),
            options: "users".to_string(),
        };
        let serialized = query_result.serialize();
        let deserialized = QueryResult::deserialize(&serialized).unwrap();

        if let QueryResult::SchemaChange {
            change_type,
            target,
            options,
        } = deserialized
        {
            assert_eq!(change_type, "CREATED");
            assert_eq!(target, "TABLE");
            assert_eq!(options, "users");
        } else {
            panic!("Expected QueryResult::SchemaChange");
        }
    }

    #[test]
    fn test_queryresult_rows_serialize_and_deserialize() {
        let row1 = row(&["col1", "col2"]).unwrap();
        let row2 = row(&["val1", "val2"]).unwrap();

        let metadata = metadata();
        let query_result = QueryResult::Rows(metadata, vec![row1, row2]);

        let serialized = query_result.serialize();
        let deserialized = QueryResult::deserialize(&serialized).unwrap();

        if let QueryResult::Rows(_, rows) = deserialized {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0], row(&["col1", "col2"]).unwrap());
            assert_eq!(rows[1], row(&["val1", "val2"]).unwrap());
        } else {
            panic!("Expected QueryResult::Rows");
        }
    }

    /*
    #[test]
    fn test_queryresult_to_vec() {
        let row1 = row(&vec!["col1", "col2"]).unwrap();
        let row2 = row(&vec!["val1", "val2"]).unwrap();

        let metadata = metadata();
        let query_result = QueryResult::Rows(metadata, vec![row1, row2]);

        let vec_result = query_result.to_vec().unwrap();
        assert_eq!(vec_result.len(), 2);
        assert_eq!(vec_result[0], "col1, col2");
        assert_eq!(vec_result[1], "val1, val2");
    }
        */

    #[test]
    fn test_queryresult_prepared_serialize_and_deserialize() {
        let query_result = QueryResult::Prepared {
            id: vec![1, 2, 3],
            metadata: Default::default(),
            result_metadata: Default::default(),
        };

        let serialized = query_result.serialize();
        let deserialized = QueryResult::deserialize(&serialized).unwrap();

        if let QueryResult::Prepared { id, .. } = deserialized {
            assert_eq!(id, vec![1, 2, 3]);
        } else {
            panic!("Expected QueryResult::Prepared");
        }
    }
}
