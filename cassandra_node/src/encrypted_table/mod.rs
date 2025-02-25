mod serde_table;
pub mod table;
use common::security::base_encryption_functions::{decrypt, encrypt};
use std::{
    collections::HashMap,
    env,
    fs::{self, File},
    io::{self, BufWriter, Write},
};
use table::{Partition, Table};

use crate::query_parser::expression::Expression;

#[derive(Debug, Clone)]
/// A struct representing an encrypted table that can be manipulated using CRUD operations.
pub struct EncryptedTable {
    table: Vec<u8>, // Serialized and encrypted table data
    key: u64,       // Encryption key for securing the table data
}

impl EncryptedTable {
    /// Creates a new `EncryptedTable` by serializing and encrypting the given `Table` instance.
    pub fn new(table: Table) -> Self {
        dotenv::dotenv().ok();
        let key: u64 = env::var("DB_KEY")
            .expect("DB_KEY no está configurada")
            .parse()
            .expect("DB_KEY must be a number");
        Self {
            table: encrypt_table(table, key),
            key: env::var("DB_KEY")
                .expect("DB_KEY no está configurada")
                .parse()
                .expect("DB_KEY must be a number"),
        }
    }

    /// Inserts a new row into the table with the given values.
    ///
    /// # Parameters
    /// - `values`: A `HashMap` containing the column names and their respective values.
    ///
    /// # Returns
    /// - `Ok(())` on success, or a descriptive `Err(String)` on failure.
    pub fn insert(&mut self, values: HashMap<String, String>) -> Result<(), String> {
        self.crud_operation(|table| table.insert(values))
    }

    /// Updates rows in the table that match the given `partition_key` using the specified `Expression`.
    ///
    /// # Parameters
    /// - `partition_key`: A `HashMap` identifying the rows to update.
    /// - `values`: An `Expression` representing the update operation.
    ///
    /// # Returns
    /// - `Ok(())` on success, or a descriptive `Err(String)` on failure.
    pub fn update(
        &mut self,
        partition_key: HashMap<String, String>,
        values: &Expression,
    ) -> Result<(), String> {
        self.crud_operation(|table| table.update(partition_key, values))
    }

    /// Deletes rows from the table that satisfy the given condition.
    ///
    /// # Parameters
    /// - `condition`: An `Expression` specifying which rows to delete.
    ///
    /// # Returns
    /// - `Ok(())` on success, or a descriptive `Err(String)` on failure.
    pub fn delete(&mut self, condition: &Expression) -> Result<(), String> {
        self.crud_operation(|table| table.delete(condition))
    }

    /// Deletes a partition from the table that matches the given partition keys.
    /// 
    /// # Parameters
    /// * `partition_keys` - A `Vec<String>` containing the partition keys to match.
    /// 
    /// # Returns
    /// * `Ok(())` on success, or a descriptive `Err(String)` on failure.
    pub fn delete_partition(&mut self, partition_keys: &Vec<String>) -> Result<(), String> {
        self.crud_operation(|table| table.delete_partition(partition_keys))
    }

    // Deserializa la tabla, hace operacion, guarda tabla modificada encriptada.
    fn crud_operation<F>(&mut self, operation: F) -> Result<(), String>
    where
        F: FnOnce(&mut Table) -> Result<(), String>,
    {
        let mut table = self.decrypt_table();
        let operation_result = operation(&mut table);
        self.table = encrypt_table(table, self.key);
        operation_result
    }

    /// Displays the contents of the table by decrypting and deserializing it.
    pub fn show(&self) {
        self.decrypt_table().show()
    }

    /// Retrieves the column names that make up the partition key for the table.
    ///
    /// # Returns
    /// A `Vec<String>` containing the partition key column names.
    pub fn get_partition_key_columns(&self) -> Vec<String> {
        self.decrypt_table().get_partition_key_columns()
    }

    /// Retrieves the keyspace name of the table.
    /// 
    /// # Returns
    /// A `String` containing the keyspace name.
    pub fn get_keyspace_name(&self) -> String {
        // split the table name by the dot and get the first part
        let table = self.decrypt_table();
        let table_name = table.get_name();
        let keyspace_name = table_name.split('.').collect::<Vec<&str>>()[0];
        keyspace_name.to_string()
    }

    /// Retrieves the rows from the partition that match the given partition keys.
    /// 
    /// # Parameters
    /// * `partition_keys` - A `Vec<String>` containing the partition keys to match.
    /// 
    /// # Returns
    /// A `Vec<HashMap<String, String>>` containing the rows that match the partition keys.
    pub fn get_rows_from_partition(&self, partition_keys: &Vec<String>) -> Vec<HashMap<String, String>> {
        self.decrypt_table().get_rows_from_partition(partition_keys)
    }

    /// Checks if the table contains the specified row.
    ///
    /// # Parameters
    /// - `row`: A `HashMap` representing the row to search for.
    ///
    /// # Returns
    /// - `true` if the row exists in the table, otherwise `false`.
    pub fn contains_row(&self, row: &HashMap<String, String>) -> bool {
        self.decrypt_table().contains_row(row)
    }

    /// Decrypts and deserializes the table, returning the underlying `Table` instance.
    pub fn get_table(&self) -> Table {
        self.decrypt_table()
    }

    /// Retrieves the partitions of the table.
    pub fn get_partitions(&self) -> HashMap<Vec<String>, Partition> {
        self.decrypt_table().get_partitions()
    }

    /// Decrypts and deserializes the table for internal use.
    ///
    /// # Returns
    /// The decrypted `Table` instance.
    fn decrypt_table(&self) -> Table {
        let decrypted_table = decrypt(&self.table, self.key);
        Table::from_bytes(&decrypted_table).expect("Error deserializing table")
    }

    /// Writes the encrypted table to disk at the specified path.
    /// 
    /// # Parameters
    /// * `path` - The path to write the table to.
    /// * `table_name` - The name of the table.
    /// 
    /// # Returns
    /// An `io::Result` indicating the success of the operation.
    pub fn write_to_disk(&self, path: &str, table_name: &str) -> io::Result<()> {
        // Create the directory if it doesn't exist
        fs::create_dir_all(path)?;

        // Construct the full file path
        let file_name = format!("{}/{}", path, table_name);
        let temp_file_name = format!("{}.tmp", file_name);

        // Create or overwrite the temporary file
        let file = File::create(&temp_file_name)?;
        let mut writer = BufWriter::new(file);

        // Write the encrypted table to the temporary file
        writer.write_all(&self.table)?;

        // Rename the temporary file to the final file
        fs::rename(temp_file_name, file_name)?;

        Ok(())
    }

    /// Loads an encrypted table from disk.
    /// 
    /// # Parameters
    /// * `node_id` - The ID of the node that owns the table.
    /// * `file_name` - The name of the file containing the table.
    /// 
    /// # Returns
    /// An `io::Result` containing the loaded `EncryptedTable` instance.
    pub fn load_table(node_id: &str, file_name: &str) -> io::Result<Self> {
        let path = format!("./data/{}/{}", node_id, file_name);
        dotenv::dotenv().ok();
        Ok(Self {
            table: fs::read(path)?,
            key: env::var("DB_KEY")
                .expect("DB_KEY no está configurada")
                .parse()
                .expect("DB_KEY must be a number"),
        })
    }
}

/// Encrypts a table by serializing it to bytes and applying encryption.
///
/// # Parameters
/// - `table`: The `Table` instance to encrypt.
/// - `key`: The encryption key.
///
/// # Returns
/// A `Vec<u8>` representing the encrypted table data.
fn encrypt_table(table: Table, key: u64) -> Vec<u8> {
    let bytes = table.to_bytes();
    encrypt(&bytes, key)
}
