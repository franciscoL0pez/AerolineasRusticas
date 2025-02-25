use std::{
    collections::{BTreeMap, HashMap},
    io::{self, Cursor, Read},
};

use super::table::{Partition, Table};

impl Table {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        write_string(&mut buffer, &self.table_name);
        write_string_list(&mut buffer, &self.partition_key_columns);
        write_string_list(&mut buffer, &self.clustering_key_columns);
        write_string_map(&mut buffer, &self.columns);

        // Write the number of partitions
        let partition_count = self.partitions.len() as u16;
        write_short(&mut buffer, partition_count);

        for (key, partition) in &self.partitions {
            write_string_list(&mut buffer, key);
            write_partition(&mut buffer, partition);
        }

        buffer
    }

    pub fn from_bytes(bytes: &[u8]) -> io::Result<Table> {
        let mut cursor = Cursor::new(bytes);

        let table_name = read_string(&mut cursor)?;
        let partition_key_columns = read_string_list(&mut cursor)?;
        let clustering_key_columns = read_string_list(&mut cursor)?;
        let columns = read_string_map(&mut cursor)?;

        // Read the number of partitions
        let partition_count = read_short(&mut cursor)? as usize;
        let mut partitions = HashMap::with_capacity(partition_count);

        for _ in 0..partition_count {
            let partition_key = read_string_list(&mut cursor)?;
            let partition = read_partition(&mut cursor)?;
            partitions.insert(partition_key, partition);
        }

        Ok(Table {
            table_name,
            partition_key_columns,
            clustering_key_columns,
            columns,
            partitions,
        })
    }
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

pub fn write_string_list(buffer: &mut Vec<u8>, strings: &[String]) {
    let n = strings.len() as u16;
    write_short(buffer, n); // write [short] n
    for string in strings {
        write_string(buffer, string);
    }
}

// Write a [string map]
pub fn write_string_map(buffer: &mut Vec<u8>, kv_pairs: &Vec<(String, String)>) {
    let n = kv_pairs.len() as u16;
    write_short(buffer, n); // write [short] n
    for (key, value) in kv_pairs {
        write_string(buffer, key);
        write_string(buffer, value);
    }
}

// Write a [partition]
pub fn write_partition(buffer: &mut Vec<u8>, partition: &Partition) {
    write_string_list(buffer, &partition.clustering_key_columns);

    // Write the number of rows (entries) in the partition
    let row_count = partition.rows.len() as u16;
    write_short(buffer, row_count);

    // Write each row (key-value pairs)
    for (key, value) in &partition.rows {
        write_string_list(buffer, key);

        // Write the inner map's key-value pairs
        write_string_map(
            buffer,
            &value.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        );
    }
}

pub fn read_short(cursor: &mut Cursor<&[u8]>) -> io::Result<u16> {
    let mut buf = [0; 2];
    cursor.read_exact(&mut buf)?;
    Ok(u16::from_be_bytes(buf))
}

pub fn read_string(cursor: &mut Cursor<&[u8]>) -> io::Result<String> {
    let len = read_short(cursor)? as usize;
    let mut buf = vec![0; len];
    cursor.read_exact(&mut buf)?;
    Ok(String::from_utf8(buf).unwrap())
}

fn read_string_list(cursor: &mut Cursor<&[u8]>) -> io::Result<Vec<String>> {
    let len = read_short(cursor)?;
    let mut list = Vec::with_capacity(len as usize);
    for _ in 0..len {
        list.push(read_string(cursor)?);
    }
    Ok(list)
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

// Read a [partition]
pub fn read_partition(cursor: &mut Cursor<&[u8]>) -> io::Result<Partition> {
    let clustering_key_columns = read_string_list(cursor)?;
    let row_count = read_short(cursor)? as usize;

    let mut rows = BTreeMap::new();
    for _ in 0..row_count {
        let key = read_string_list(cursor)?;
        let value = read_string_map(cursor)?
            .into_iter()
            .collect::<HashMap<String, String>>();
        rows.insert(key, value);
    }

    Ok(Partition {
        clustering_key_columns,
        rows,
    })
}
