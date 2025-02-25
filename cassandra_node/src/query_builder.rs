use std::collections::HashMap;

use chrono::Utc;

use crate::encrypted_table::table::Table;
use crate::replication_strategy::ReplicationStrategy;

pub fn create_keyspace_query(
    keyspace_name: &str,
    replication_strategy: ReplicationStrategy,
) -> String {
    let query = format!(
        "CREATE KEYSPACE {} WITH REPLICATION = {{'class': '{}', 'replication_factor': {}}};",
        keyspace_name,
        replication_strategy.get_name(),
        replication_strategy.get_replication_factor()
    );
    query
}

pub fn create_table_query(table: &Table) -> String {
    // this one is much complicated, as it has to create the table with the correct types and the correct primary keys
    // split by . the table name to get the keyspace name
    let keyspace_name_table_name: Vec<&str> = table.get_name().split('.').collect();
    let table_name = keyspace_name_table_name[1];

    let mut query = format!("CREATE TABLE {} (", table_name);
    let columns = table.get_columns();
    let partition_key_columns = table.get_partition_key_columns();
    let clustering_key_columns = table.get_clustering_key_columns();

    for (column_name, column_type) in columns {
        query.push_str(&format!("{} {},", column_name, column_type));
    }

    query.push_str(" PRIMARY KEY (");
    
    for (i, column_name) in partition_key_columns.iter().enumerate() {
        query.push_str(column_name);
        if i != partition_key_columns.len() - 1 {
            query.push_str(", ");
        } else {
            query.push_str("), ");
        }
    }

    for (i, column_name) in clustering_key_columns.iter().enumerate() {
        query.push_str(column_name);
        if i != clustering_key_columns.len() - 1 {
            query.push_str(", ");
        } else {
            query.push_str(")");
        }
    }
    query.push_str(");");

    query
}

pub fn insert_message_from_row_and_tablename(
    row: &HashMap<String, String>,
    table_name: &str,
) -> String {
    let mut insert_str = format!("INSERT INTO {} (", table_name);
    let mut values_str = "VALUES (".to_string();

    for (i, (column, value)) in row.iter().enumerate() {
        insert_str.push_str(column);
        values_str.push('\'');
        values_str.push_str(value);
        values_str.push('\'');
        if i < row.len() - 1 {
            insert_str.push_str(", ");
            values_str.push_str(", ");
        }
    }
    insert_str.push_str(") ");
    values_str.push_str(");");

    insert_str.push_str(values_str.as_str());
    insert_str
}

/// Extracts  the columns (in order) from an `INSERT` statement string in CQL.
///
/// # Parameters
/// - `insert_str`: The `INSERT` statement string.
///
/// # Returns
/// A `Vec<String>` containing the columns in order.
///
pub fn add_timestamp_to_insert_message(insert_str: &str) -> String {
    let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let insert_str_before_first_closing_parenthesis =
        insert_str.split(")").collect::<Vec<&str>>()[0];
    let insert_str_after_values = insert_str.split("VALUES").collect::<Vec<&str>>()[1];
    let vector_of_values = insert_str_after_values.split(")").collect::<Vec<&str>>();

    let mut insert_str_with_timestamp = insert_str_before_first_closing_parenthesis.to_string();
    insert_str_with_timestamp.push_str(", _timestamp) VALUES");
    // For each value, add a comma and the value except for the last, which is a ;
    for (i, value) in vector_of_values.iter().enumerate() {
        insert_str_with_timestamp.push_str(value);
        if i < vector_of_values.len() - 1 {
            insert_str_with_timestamp.push_str(", '");
            insert_str_with_timestamp.push_str(&timestamp);
            insert_str_with_timestamp.push_str("')");
        }
    }

    insert_str_with_timestamp
}

/// Adds a `_timestamp` field with the current timestamp to an `UPDATE` SQL statement.
///
/// # Parameters
/// - `update_str`: The `UPDATE` statement string.
///
/// # Returns
/// A `String` containing the `UPDATE` statement with the `_timestamp` field added.
///
pub fn add_timestamp_to_update_message(update_str: &str) -> String {
    let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let update_str_before_set = update_str.split("SET").collect::<Vec<&str>>()[0];
    let update_str_after_set = update_str.split("SET").collect::<Vec<&str>>()[1];

    let mut update_str_with_timestamp = update_str_before_set.to_string();
    update_str_with_timestamp.push_str("SET ");
    update_str_with_timestamp.push_str("_timestamp = '");
    update_str_with_timestamp.push_str(&timestamp);
    update_str_with_timestamp.push_str("',");
    update_str_with_timestamp.push_str(update_str_after_set);
    update_str_with_timestamp
}