use crate::frame::messages::consistency_level::ConsistencyLevel;
use crate::frame::messages::notation::{
    read_byte, read_bytes, read_consistency, read_int, read_long, read_long_string, read_short,
    read_string, write_byte, write_bytes, write_consistency, write_int, write_long,
    write_long_string, write_short, write_string,
};
use std::io;
use std::io::Cursor;

#[derive(Copy, Clone)]
enum QueryFlag {
    Values = 0x01,
    SkipMetadata = 0x02,
    PageSize = 0x04,
    WithPagingState = 0x08,
    WithSerialConsistency = 0x10,
    WithDefaultTimestamp = 0x20,
    WithNamesForValues = 0x40,
}

impl QueryFlag {
    fn is_set(&self, flags: u8) -> bool {
        flags & (*self as u8) != 0
    }
}

#[derive(Debug, Clone)]
pub struct Query {
    pub query_string: String,
    pub consistency_level: ConsistencyLevel,
    pub values: Option<Vec<(Option<String>, Vec<u8>)>>,
    pub skip_metadata: bool,
    pub result_page_size: Option<i32>,
    pub paging_state: Option<Vec<u8>>,
    pub serial_consistency: Option<ConsistencyLevel>,
    pub time_stamp: Option<i64>,
}

impl Query {
    pub fn default(query_string: String, consistency_level: ConsistencyLevel) -> Self {
        Self {
            query_string,
            consistency_level,
            values: None,
            skip_metadata: true,
            result_page_size: None,
            paging_state: None,
            serial_consistency: None,
            time_stamp: None,
        }
    }

    pub fn deserialize(body: &[u8]) -> io::Result<Self> {
        let mut cursor = Cursor::new(body);

        let query_string = read_long_string(&mut cursor)?;
        let consistency = read_consistency(&mut cursor)?;

        let flags = read_byte(&mut cursor)?;

        let values = if QueryFlag::Values.is_set(flags) {
            Some(deserialize_values(
                &mut cursor,
                QueryFlag::WithNamesForValues.is_set(flags),
            )?)
        } else {
            None
        };

        let skip_metadata = QueryFlag::SkipMetadata.is_set(flags);

        let result_page_size =
            read_optional_value(&mut cursor, QueryFlag::PageSize.is_set(flags), read_int)?;
        let paging_state = read_optional_value(
            &mut cursor,
            QueryFlag::WithPagingState.is_set(flags),
            read_bytes,
        )?;
        let serial_consistency = read_optional_value(
            &mut cursor,
            QueryFlag::WithSerialConsistency.is_set(flags),
            read_consistency,
        )?;
        let time_stamp = read_optional_value(
            &mut cursor,
            QueryFlag::WithDefaultTimestamp.is_set(flags),
            read_long,
        )?;

        Ok(Query {
            query_string,
            consistency_level: consistency,
            values,
            skip_metadata,
            result_page_size,
            paging_state,
            serial_consistency,
            time_stamp,
        })
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut body = Vec::new();

        write_long_string(&mut body, &self.query_string);
        write_consistency(&mut body, self.consistency_level);
        write_byte(&mut body, self.serialize_flags());

        if let Some(values) = &self.values {
            write_short(&mut body, values.len() as u16);
            for (name, value) in values {
                if let Some(name_str) = name {
                    write_string(&mut body, name_str);
                }
                write_bytes(&mut body, value);
            }
        }

        if let Some(page_size) = self.result_page_size {
            write_int(&mut body, page_size);
        }
        if let Some(paging_state) = &self.paging_state {
            write_bytes(&mut body, paging_state);
        }
        if let Some(serial_consistency) = self.serial_consistency {
            write_consistency(&mut body, serial_consistency);
        }
        if let Some(time_stamp) = self.time_stamp {
            write_long(&mut body, time_stamp);
        }

        body
    }

    fn serialize_flags(&self) -> u8 {
        let mut flags = 0u8;
        if let Some(values) = &self.values {
            flags |= QueryFlag::Values as u8;
            if values.iter().any(|(name, _)| name.is_some()) {
                flags |= QueryFlag::WithNamesForValues as u8;
            }
        }
        if self.skip_metadata {
            flags |= QueryFlag::SkipMetadata as u8;
        }
        if self.result_page_size.is_some() {
            flags |= QueryFlag::PageSize as u8;
        }
        if self.paging_state.is_some() {
            flags |= QueryFlag::WithPagingState as u8;
        }
        if self.serial_consistency.is_some() {
            flags |= QueryFlag::WithSerialConsistency as u8;
        }
        if self.time_stamp.is_some() {
            flags |= QueryFlag::WithDefaultTimestamp as u8;
        }
        flags
    }
}

fn read_optional_value<T, F>(
    cursor: &mut Cursor<&[u8]>,
    flag_is_set: bool,
    read_fn: F,
) -> Result<Option<T>, io::Error>
where
    F: Fn(&mut Cursor<&[u8]>) -> Result<T, io::Error>,
{
    if !flag_is_set {
        return Ok(None);
    }
    Ok(Some(read_fn(cursor)?))
}

fn deserialize_values(
    cursor: &mut Cursor<&[u8]>,
    with_names: bool,
) -> io::Result<Vec<(Option<String>, Vec<u8>)>> {
    let n = read_short(cursor)?;
    let mut values = Vec::with_capacity(n as usize);
    for _ in 0..n {
        let name = read_optional_value(cursor, with_names, read_string)?;
        let value = read_bytes(cursor)?;
        values.push((name, value));
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::messages::consistency_level::ConsistencyLevel;

    #[test]
    fn test_query_default() {
        let query_string = "SELECT * FROM test".to_string();
        let consistency = ConsistencyLevel::One;
        let query = Query::default(query_string.clone(), consistency);

        assert_eq!(query.query_string, query_string);
        assert_eq!(query.consistency_level, consistency);
        assert!(query.values.is_none());
        assert!(query.skip_metadata);
        assert!(query.result_page_size.is_none());
        assert!(query.paging_state.is_none());
        assert!(query.serial_consistency.is_none());
        assert!(query.time_stamp.is_none());
    }

    #[test]
    fn test_query_serialize_deserialize() {
        let query_string = "SELECT * FROM test".to_string();
        let consistency = ConsistencyLevel::One;

        // Create query object
        let query = Query {
            query_string: query_string.clone(),
            consistency_level: consistency,
            values: Some(vec![(Some("id".to_string()), vec![1, 2, 3])]),
            skip_metadata: false,
            result_page_size: Some(100),
            paging_state: Some(vec![4, 5, 6]),
            serial_consistency: Some(ConsistencyLevel::Serial),
            time_stamp: Some(1627550738),
        };

        // Serialize query
        let serialized = query.serialize();

        // Deserialize the serialized data
        let deserialized_query = Query::deserialize(&serialized).unwrap();

        // Ensure the deserialized query matches the original
        assert_eq!(deserialized_query.query_string, query_string);
        assert_eq!(deserialized_query.consistency_level, consistency);
        assert_eq!(
            deserialized_query.values,
            Some(vec![(Some("id".to_string()), vec![1, 2, 3])])
        );
        assert!(!deserialized_query.skip_metadata);
        assert_eq!(deserialized_query.result_page_size, Some(100));
        assert_eq!(deserialized_query.paging_state, Some(vec![4, 5, 6]));
        assert_eq!(
            deserialized_query.serial_consistency,
            Some(ConsistencyLevel::Serial)
        );
        assert_eq!(deserialized_query.time_stamp, Some(1627550738));
    }

    #[test]
    fn test_query_flags() {
        let query_string = "SELECT * FROM test".to_string();
        let consistency = ConsistencyLevel::One;

        // Query with all flags enabled
        let query = Query {
            query_string,
            consistency_level: consistency,
            values: Some(vec![(Some("id".to_string()), vec![1, 2, 3])]),
            skip_metadata: true,
            result_page_size: Some(100),
            paging_state: Some(vec![4, 5, 6]),
            serial_consistency: Some(ConsistencyLevel::Serial),
            time_stamp: Some(1627550738),
        };

        let flags = query.serialize_flags();

        // Ensure all flags are properly set
        assert_eq!(flags & QueryFlag::Values as u8, QueryFlag::Values as u8);
        assert_eq!(
            flags & QueryFlag::WithNamesForValues as u8,
            QueryFlag::WithNamesForValues as u8
        );
        assert_eq!(
            flags & QueryFlag::SkipMetadata as u8,
            QueryFlag::SkipMetadata as u8
        );
        assert_eq!(flags & QueryFlag::PageSize as u8, QueryFlag::PageSize as u8);
        assert_eq!(
            flags & QueryFlag::WithPagingState as u8,
            QueryFlag::WithPagingState as u8
        );
        assert_eq!(
            flags & QueryFlag::WithSerialConsistency as u8,
            QueryFlag::WithSerialConsistency as u8
        );
        assert_eq!(
            flags & QueryFlag::WithDefaultTimestamp as u8,
            QueryFlag::WithDefaultTimestamp as u8
        );
    }

    #[test]
    fn test_read_optional_value() {
        // Test case where flag is set
        let body = vec![0x01, 0x02, 0x03, 0x04];
        let mut cursor = Cursor::new(body.as_slice());
        let result: Option<i32> = read_optional_value(&mut cursor, true, read_int).unwrap();
        assert_eq!(result, Some(16909060)); // Big-endian conversion of bytes 0x01, 0x02, 0x03, 0x04

        let binding = vec![0x01u8];
        let flags = binding.as_slice();
        let mut cursor = Cursor::new(flags);
        let result: Option<i32> = read_optional_value(&mut cursor, false, read_int).unwrap();
        assert_eq!(result, None);
    }
}
