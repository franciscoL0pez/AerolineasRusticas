mod option;
mod spec;

use crate::frame::messages::notation::{
    read_bytes, read_int, write_bytes, write_int,
};
use crate::frame::messages::query_result::metadata::spec::Spec;
use std::io::{self, Cursor};

#[repr(i32)]
#[derive(Copy, Clone)]
enum MetadataFlags {
    GlobalTablesSpec = 0x0001,
    HasMorePages = 0x0002,
    NoMetadata = 0x0004,
}

impl MetadataFlags {
    pub fn value(self) -> i32 {
        self as i32
    }
    fn is_set(&self, flags: i32) -> bool {
        (flags & (*self as i32)) != 0
    }
}

#[derive(Debug, Clone)]
pub struct Metadata {
    columns_count: i32,
    paging_state: Option<Vec<u8>>,
    spec: Option<Spec>,
}

impl Metadata {
    pub fn deserialize(cursor: &mut Cursor<&[u8]>) -> io::Result<Self> {
        let flags = read_int(cursor)?;
        let columns_count = read_int(cursor)?;

        let paging_state = if MetadataFlags::HasMorePages.is_set(flags) {
            Some(read_bytes(cursor)?)
        } else {
            None
        };

        let spec = if MetadataFlags::NoMetadata.is_set(flags) {
            None
        } else {
            Some(Spec::deserialize(
                cursor,
                MetadataFlags::GlobalTablesSpec.is_set(flags),
                columns_count as usize,
            )?)
        };

        Ok(Metadata {
            columns_count,
            paging_state,
            spec,
        })
    }

    pub fn serialize(&self, buffer: &mut Vec<u8>) {
        let mut flags = 0i32;
        if self.paging_state.is_some() {
            flags |= MetadataFlags::HasMorePages.value();
        }
        match self.spec {
            Some(Spec::Global(..)) => flags |= MetadataFlags::GlobalTablesSpec.value(),
            None => flags |= MetadataFlags::NoMetadata.value(),
            _ => {}
        }
        write_int(buffer, flags);
        write_int(buffer, self.columns_count);
        if let Some(paging_state) = &self.paging_state {
            write_bytes(buffer, paging_state);
        }
        if let Some(spec) = &self.spec {
            spec.serialize(buffer)
        }
    }

    pub fn get_columns_count(&self) -> i32 {
        self.columns_count
    }
}

impl Default for Metadata {
    fn default() -> Self {
        Metadata {
            columns_count: 1,
            paging_state: None,
            spec: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::messages::notation::{write_bytes, write_int, write_string};

    #[test]
    fn test_metadata_parse_with_paging_state_and_spec() {
        // Create a buffer representing valid metadata with paging state and spec
        let mut buffer = vec![];

        // Simulate setting flags for HasMorePages and GlobalTablesSpec
        write_int(
            &mut buffer,
            MetadataFlags::HasMorePages.value() | MetadataFlags::GlobalTablesSpec.value(),
        );
        write_int(&mut buffer, 0); // columns_count = 0

        // Add some paging state bytes
        let paging_state = vec![1, 2, 3, 4];
        write_bytes(&mut buffer, &paging_state);

        // Add mock Spec data (already tested in Spec unit tests)
        let mut mock_spec = vec![];
        write_string(&mut mock_spec, "ks");
        write_string(&mut mock_spec, "table");
        buffer.extend_from_slice(&mock_spec);

        let mut cursor = Cursor::new(buffer.as_slice());

        // Parse the metadata
        let metadata = Metadata::deserialize(&mut cursor).expect("Should parse successfully");

        // Verify parsed values
        assert_eq!(metadata.columns_count, 0);
        assert_eq!(metadata.paging_state.as_ref().unwrap(), &paging_state);
        assert!(metadata.spec.is_some());
    }

    #[test]
    fn test_metadata_parse_no_paging_state_no_spec() {
        // Create a buffer representing metadata with no paging state and no spec
        let mut buffer = vec![];

        // Simulate setting flags for NoMetadata (no spec)
        write_int(&mut buffer, MetadataFlags::NoMetadata.value());
        write_int(&mut buffer, 5); // columns_count = 5

        // Create a cursor from the buffer
        let mut cursor = Cursor::new(buffer.as_slice());

        // Parse the metadata
        let metadata = Metadata::deserialize(&mut cursor).expect("Should parse successfully");

        // Verify parsed values
        assert_eq!(metadata.columns_count, 5);
        assert!(metadata.paging_state.is_none());
        assert!(metadata.spec.is_none());
    }

    #[test]
    fn test_metadata_write_with_paging_state_and_spec() {
        let paging_state = Some(vec![1, 2, 3, 4]);
        let mock_spec = Some(Spec::NotGlobal(vec![])); // Example Spec value for testing

        let metadata = Metadata {
            columns_count: 0,
            paging_state: paging_state.clone(),
            spec: mock_spec,
        };

        let mut buffer = vec![];
        metadata.serialize(&mut buffer);

        // Parse the buffer back into metadata and verify
        let mut cursor = Cursor::new(buffer.as_slice());
        let parsed_metadata =
            Metadata::deserialize(&mut cursor).expect("Should parse successfully");

        assert_eq!(parsed_metadata.columns_count, 0);
        assert_eq!(parsed_metadata.paging_state, paging_state);
        assert!(parsed_metadata.spec.is_some());
    }

    #[test]
    fn test_metadata_write_no_paging_state_no_spec() {
        let metadata = Metadata {
            columns_count: 5,
            paging_state: None,
            spec: None,
        };

        let mut buffer = vec![];
        metadata.serialize(&mut buffer);

        // Parse the buffer back into metadata and verify
        let mut cursor = Cursor::new(buffer.as_slice());
        let parsed_metadata =
            Metadata::deserialize(&mut cursor).expect("Should parse successfully");

        assert_eq!(parsed_metadata.columns_count, 5);
        assert!(parsed_metadata.paging_state.is_none());
        assert!(parsed_metadata.spec.is_none());
    }
}
