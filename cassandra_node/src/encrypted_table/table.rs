use crate::query_parser::{expression::evaluate_expression, expression::Expression};
use serde::Deserialize;
use std::{collections::{BTreeMap, HashMap}, fs::{self, File}, io::{BufWriter, Write}};

/// This struct represents a table including its parts.
/// 
/// 
#[derive(Debug, Clone, Deserialize)]
pub struct Table {
    /// A table
    pub table_name: String,
    pub partition_key_columns: Vec<String>,
    pub clustering_key_columns: Vec<String>,
    pub columns: Vec<(String, String)>,
    pub partitions: HashMap<Vec<String>, Partition>, // partition key: partition
}

/// This struct represents a partition of a table.
/// 
/// 
#[derive(Debug, Clone, Deserialize)]
pub struct Partition {
    // holds data with the same partition key
    // rows with specific partition key. Hashmap column key: value.
    // rows are ordered based on clustering keys
    pub clustering_key_columns: Vec<String>,
    pub rows: BTreeMap<Vec<String>, HashMap<String, String>>,
}

impl Table {
    // The code supposes that all the columns are unique, and partition/clustering key columns do exist and exist in columns vector
    pub fn new(
        table_name: String,
        partition_key_columns: Vec<String>,
        clustering_key_columns: Vec<String>,
        columns: Vec<(String, String)>,
    ) -> Self {
        let mut columns = columns;
        // add _timestamp
        columns.push(("_timestamp".to_string(), "String".to_string()));
        Table {
            table_name,
            partition_key_columns,
            clustering_key_columns,
            columns,
            partitions: HashMap::new(),
        }
    }

    /// Verifies if the table contains a row.
    /// 
    /// #Parameters
    /// - `row`: Hashmap that contains the data of a row.
    /// 
    /// # Returns
    /// True or false.
    /// 
    pub fn contains_row(&self, row: &HashMap<String, String>) -> bool {
        for partition in self.partitions.values() {
            for partition_row in partition.rows.values() {
                if partition_row == row {
                    return true;
                }
            }
        }
        false
    }

    /// Gets the columns of the table.
    pub fn get_columns(&self) -> &Vec<(String, String)> {
        &self.columns
    }

    /// Gets the name of the table.
    pub fn get_name(&self) -> &String {
        &self.table_name
    }

    /// Inserts a row on the table.
    /// 
    /// #Parameters
    /// - `row`: Hashmap that contains the data of a row.
    ///
    pub fn insert(&mut self, row: HashMap<String, String>) -> Result<(), String> {
        for column in row.keys() {
            if !self.columns.iter().any(|(col, _)| col == column) {
                return Err(format!("Column {} does not exist", column));
            }
        }
        let mut partition_keys: Vec<String> = vec![];
        for partition_key in &self.partition_key_columns {
            // get partition keys from row
            if let Some(value) = row.get(partition_key) {
                partition_keys.push(value.clone());
            } else {
                // if partition key is missing in the row, return error
                return Err(format!("Partition key {} is missing", partition_key));
            }
        }

        if let Some(partition) = self.partitions.get_mut(&partition_keys) {
            // if a partition for those partition keys already exists, insert row into partition

            partition.insert(row)?;
        } else {
            // if not, create a new partition, insert row into partition, and insert partition into table
            let mut partition = Partition::new(self.clustering_key_columns.clone());
            partition.insert(row)?;
            self.partitions.insert(partition_keys, partition);
        }
        Ok(())
    }

    /// Updates a row on the table given a condition.
    /// 
    /// #Parameters
    /// - `values_to_update`: Hashmap that contains the values to update on the table.
    /// - `condition`: Contains the condition to search on the table.
    ///
    pub fn update(
        &mut self,
        values_to_update: HashMap<String, String>,
        condition: &Expression,
    ) -> Result<(), String> {
        for partition in self.partitions.values_mut() {
            for row in partition.rows.values_mut() {
                let result = evaluate_expression(condition, row);
                if let Ok(true) = result {
                    for (column, value) in values_to_update.iter() {
                        if self.columns.iter().any(|(col, _)| col == column) {
                            row.insert(column.clone(), value.clone());
                        } else {
                            return Err(format!("Column {} does not exist", column));
                        }
                    }
                } else if let Err(e) = result {
                    return Err(e.to_string());
                }
            }
        }
        Ok(())
    }

    /// Deletes a row on the table given a condition.
    /// 
    /// #Parameters
    /// - `condition`: Contains the condition to search on the table.
    ///
    pub fn delete(&mut self, condition: &Expression) -> Result<(), String> {
        for partition in self.partitions.values_mut() {
            let mut rows_to_delete = vec![];
            for (key, row) in partition.rows.iter() {
                let result = evaluate_expression(condition, row);
                if let Ok(true) = result {
                    rows_to_delete.push(key.clone());
                } else if let Err(e) = result {
                    return Err(e.to_string());
                }
            }
            for row_key in rows_to_delete {
                partition.rows.remove(&row_key);
            }
        }
        Ok(())
    }

    /// Gets the partitions of the table.
    pub fn get_partitions(&self) -> HashMap<Vec<String>, Partition> {
        self.partitions.clone()
    }

    /// Gets the rows of the table given the partition keys.
    pub fn get_rows_from_partition(&self, partition_keys: &Vec<String>) -> Vec<HashMap<String, String>> {
        if let Some(partition) = self.partitions.get(partition_keys) {
            return partition.get_vector_of_rows();
        }
        vec![]
    }

    /// Gets the columns of the partition keys. 
    pub fn get_partition_key_columns(&self) -> Vec<String> {
        self.partition_key_columns.clone()
    }

    /// Gets the clustering key columns.
    pub fn get_clustering_key_columns(&self) -> Vec<String> {
        self.clustering_key_columns.clone()
    }

    /// Gets a vector containing the rows of the table.
    pub fn get_vector_of_rows(&self) -> Vec<HashMap<String, String>> {
        let mut rows = vec![];
        for partition in self.partitions.values() {
            rows.append(&mut partition.get_vector_of_rows());
        }
        rows
    }

    /// Prints the table ando information.
    pub fn show(&self) {
        println!("Table: {}", self.table_name);
        println!("Partition key columns: {:?}", self.partition_key_columns);
        for partition in self.partitions.values() {
            for row in partition.rows.values() {
                for (column, _) in &self.columns {
                    if let Some(value) = row.get(column) {
                        print!("{}: {}, ", column, value);
                    } else {
                        print!("{}: None, ", column);
                    }
                }
            }
        }
    }

    /// Returns the length of the table.
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        let mut len = 0;
        for partition in self.partitions.values() {
            len += partition.rows.len();
        }
        len
    }

    /// Gets a vector of mutable references to rows that match the query values.
    /// 
    /// #Parameters
    /// - 'query_values': Contains the query values to search.
    /// 
    /// #Returns
    ///- Returns the matching rows.
    pub fn get_matching_rows(
        &self,
        query_values: HashMap<String, String>,
    ) -> Vec<HashMap<String, String>> {
        let mut partition_keys: Vec<String> = vec![];
        let mut matching_rows = vec![];
        for partition_key in &self.partition_key_columns {
            if let Some(value) = query_values.get(partition_key) {
                partition_keys.push(value.clone());
            }
        }
        if let Some(partition) = self.partitions.get(&partition_keys) {
            if partition_keys.len() == query_values.len() {
                // if all partition keys are only keys in the query, return all rows in the partition
                return partition.rows.values().cloned().collect();
            }
            // if there are other keys in the querys, return only rows that match the query
            for row in partition.rows.values() {
                if row_matches_query(row, &query_values) {
                    matching_rows.push(row.clone());
                }
            }
        }
        matching_rows
    }

    /// Finds rows that match the condition.
    /// 
    /// #Parameters
    /// - 'condition': Contains the consition to evaluate on each row.
    /// 
    /// #Returns
    ///- Returns the selected rows.
    pub fn select_if(&self, condition: &Expression) -> Vec<HashMap<String, String>> {
        let mut selected_rows = vec![];
        for partition in self.partitions.values() {
            for row in partition.rows.values() {
                let result = evaluate_expression(condition, row);
                if let Ok(true) = result {
                    selected_rows.push(row.clone());
                } else if let Err(_e) = result {
                    return vec![];
                }
            }
        }
        selected_rows
    }

    /// Deletes rows that match the condition.
    /// 
    /// #Parameters
    /// - 'condition': Contains the consition to evaluate on each row.
    /// 
    pub fn delete_if(&mut self, condition: &Expression) -> Result<(), String> {
        let mut rows_to_delete = vec![];
        for partition in self.partitions.values_mut() {
            for (key, row) in partition.rows.iter() {
                let result = evaluate_expression(condition, row);
                if let Ok(true) = result {
                    rows_to_delete.push(key.clone());
                } else if let Err(e) = result {
                    return Err(e.to_string());
                }
            }
            for row_key in &rows_to_delete {
                partition.rows.remove(row_key);
            }
        }
        Ok(())
    }

    /// Deletes rows that match the query values.
    /// 
    /// #Parameters
    /// - 'query_values': Contains the query values to search.
    /// 
    pub fn delete_matching_rows(
        &mut self,
        query_values: &HashMap<String, String>,
    ) -> Result<(), String> {
        for partition in self.partitions.values_mut() {
            let mut rows_to_delete = vec![];
            for (key, row) in partition.rows.iter() {
                if row_matches_query(row, query_values) {
                    rows_to_delete.push(key.clone());
                }
            }
            for row_key in rows_to_delete {
                partition.rows.remove(&row_key);
            }
        }
        Ok(())
    }

    /// Deletes partitions.
    /// 
    /// #Parameters
    /// - 'query_partition_keys': Vector that contains the partitions to remove.
    /// 
    pub fn delete_partition(&mut self, query_partition_keys: &Vec<String>) -> Result<(), String> {
        if let Some(_partition) = self.partitions.remove(query_partition_keys) {
            Ok(())
        } else {
            Err(format!(
                "Partition with keys {:?} not found",
                query_partition_keys
            ))
        }
    }

    /// Writes the table data into a .csv file.
    /// 
    /// #Parameters
    /// - 'path': String that contains the path of the .csv file.
    /// 
    pub fn write_to_disk(&self, path: &str) -> Result<(), String> {
        // Crea el directorio si no existe
        fs::create_dir_all(path).map_err(|e| format!("Failed to create directory: {}", e))?;

        // Construye la ruta completa del archivo
        let file_name = format!("{}/{}.csv", path, self.table_name);

        // Crea o sobrescribe el archivo
        let file = File::create(&file_name).map_err(|e| format!("Failed to create file: {}", e))?;
        let mut writer = BufWriter::new(file);

        let partition_key_columns = self.get_partition_key_columns();
        let clustering_key_columns = self.get_clustering_key_columns();

        let partition_key_columns_string = partition_key_columns.join(",");

        writeln!(writer, "{}", partition_key_columns_string)
            .map_err(|e| format!("Failed to write partition_key_columns: {}", e))?;

        let clustering_key_columns_string = clustering_key_columns.join(",");

        writeln!(writer, "{}", clustering_key_columns_string)
            .map_err(|e| format!("Failed to write clustering_key_columns: {}", e))?;
        
        let columns = self.get_columns();

        let mut i = 0;
        let columns_count = columns.len();
        for (column, column_type) in columns {
            write!(writer, "{}:{}", column, column_type)
                .map_err(|e| format!("Failed to write column: {}", e))?;
            if i != columns_count - 1 {
                write!(writer, ",")
                    .map_err(|e| format!("Failed to write column: {}", e))?;
            }
            i += 1;
        }
        writeln!(writer)
            .map_err(|e| format!("Failed to write columns: {}", e))?;
        
        let column_names: Vec<String> = columns.iter().map(|(column, _)| column.clone()).collect();
        let header_string = column_names.join(",");

        writeln!(writer, "{}", header_string)
            .map_err(|e| format!("Failed to write headers: {}", e))?;

        let rows = self.get_vector_of_rows();
        
        for row in rows {
            i = 0;
            for column in column_names.iter() {
                let value = row.get(column).map_or_else(|| "".to_string(), |v| v.clone());
                write!(writer, "{}", value)
                    .map_err(|e| format!("Failed to write row: {}", e))?;
                if i != columns_count - 1 {
                    write!(writer, ",")
                        .map_err(|e| format!("Failed to write row: {}", e))?;
                }
                i += 1;
            }
            writeln!(writer)
                .map_err(|e| format!("Failed to write row: {}", e))?;
        }

        Ok(())
    }
}

/// Verifies if the row matches the query values.
/// 
/// #Parameters
/// - 'row': Contains the row of the table.
/// - 'query_values': Contains the query values to compare.
/// 
/// #Returns
///- True or false.
fn row_matches_query(
    row: &HashMap<String, String>,
    query_values: &HashMap<String, String>,
) -> bool {
    for (key, value) in query_values {
        if let Some(row_value) = row.get(key) {
            if row_value != value {
                return false;
            }
        } else {
            return false;
        }
    }
    true
}

impl Partition {
    pub fn new(clustering_key_columns: Vec<String>) -> Self {
        Partition {
            clustering_key_columns,
            rows: BTreeMap::new(),
        }
    }

    /// Inserts row into partition.
    /// 
    /// #Parameters
    /// - 'row': Contains the row to insert.
    pub fn insert(&mut self, row: HashMap<String, String>) -> Result<(), String> {
        let mut clustering_keys: Vec<String> = vec![];
        for clustering_key_column in &self.clustering_key_columns {
            if let Some(value) = row.get(clustering_key_column) {
                // if clustering key is present in the row, add it to clustering keys
                clustering_keys.push(value.clone());
            } else {
                // if clustering key is missing in the row, return error
                return Err(format!(
                    "Clustering key {} is missing",
                    clustering_key_column
                ));
            }
        }
        self.rows.insert(clustering_keys, row); // insert row into partition's btree, with clustering keys as key
        Ok(())
    }

    /// Gets the rows of the partition.
    pub fn get_rows(&self) -> &BTreeMap<Vec<String>, HashMap<String, String>> {
        &self.rows
    }

    /// Gets a vector containing the rows of the partition.
    pub fn get_vector_of_rows(&self) -> Vec<HashMap<String, String>> {
        self.rows.values().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use crate::query_parser::expression::Operand;

    use super::*;

    fn create_table(
        partition_key_columns: Vec<String>,
        clustering_key_columns: Vec<String>,
    ) -> Table {
        Table::new(
            "table_name".to_string(),
            partition_key_columns,
            clustering_key_columns,
            vec![
                ("id".to_string(), "String".to_string()),
                ("order".to_string(), "String".to_string()),
                ("data".to_string(), "String".to_string()),
            ],
        )
    }

    fn create_row(id: &str, order: &str, data: &str) -> HashMap<String, String> {
        let mut row = HashMap::new();
        row.insert("id".to_string(), id.to_string());
        row.insert("order".to_string(), order.to_string());
        row.insert("data".to_string(), data.to_string());
        row
    }

    fn insert_into_table(
        table: &mut Table,
        id: &str,
        order: &str,
        data: &str,
    ) -> Result<(), String> {
        let row = create_row(id, order, data);
        table.insert(row)
    }

    #[test]
    fn test_insert_into_new_partition() {
        let mut table = create_table(vec!["id".to_string()], vec!["order".to_string()]);

        let result = insert_into_table(&mut table, "111", "1", "data");
        assert!(result.is_ok());

        let partition_keys = vec!["111".to_string()];
        assert!(table.partitions.contains_key(&partition_keys));

        let partition = table.partitions.get(&partition_keys).unwrap();
        let clustering_keys = vec!["1".to_string()];
        assert!(partition.rows.contains_key(&clustering_keys));

        let inserted_row = partition.rows.get(&clustering_keys).unwrap();
        assert_eq!(inserted_row.get("data").unwrap(), "data");
    }

    #[test]
    fn test_when_inserting_rows_with_same_partition_keys_rows_get_inserted_ordered_by_clustering_keys(
    ) {
        let mut table = create_table(vec!["id".to_string()], vec!["order".to_string()]);
        let first_entry_row = create_row("111", "2", "first_entry");
        let second_entry_row = create_row("111", "1", "second_entry");

        let result = insert_into_table(&mut table, "111", "2", "first_entry");
        assert!(result.is_ok());
        // the following row should be inserted into the same partition, before the first inserted row
        let result = insert_into_table(&mut table, "111", "1", "second_entry");
        assert!(result.is_ok());

        let partition_keys = vec!["111".to_string()];
        let partition = table.partitions.get(&partition_keys).unwrap();
        let values: Vec<_> = partition.rows.values().collect();
        assert_eq!(values, vec![&second_entry_row, &first_entry_row]);
    }

    #[test]
    fn test_missing_partition_key() {
        let mut table = create_table(vec!["id".to_string()], vec!["order".to_string()]);
        let mut incomplete_row = HashMap::new();
        incomplete_row.insert("order".to_string(), "1".to_string()); // Partition key missing
        incomplete_row.insert("data".to_string(), "data".to_string());

        let result = table.insert(incomplete_row);

        assert!(result.is_err());
    }

    #[test]
    fn test_missing_clustering_key() {
        let mut table = create_table(vec!["id".to_string()], vec!["order".to_string()]);
        let mut row_missing_clustering_key = HashMap::new();
        row_missing_clustering_key.insert("id".to_string(), "1".to_string());
        row_missing_clustering_key.insert("data".to_string(), "data".to_string());

        let result = table.insert(row_missing_clustering_key);

        assert!(result.is_err());
    }

    #[test]
    fn test_delete_matching_rows() {
        let mut table = create_table(vec!["id".to_string()], vec!["order".to_string()]);
        let row1 = create_row("111", "1", "data1");
        let row2 = create_row("111", "2", "data2");
        let row3 = create_row("111", "3", "data3");

        let _ = table.insert(row1.clone());
        let _ = table.insert(row2.clone());
        let _ = table.insert(row3.clone());

        let mut query_values = HashMap::new();
        query_values.insert("order".to_string(), "2".to_string());
        let result = table.delete_matching_rows(&query_values);

        assert!(result.is_ok());
        assert!(table.get_vector_of_rows().contains(&row1));
        assert!(!table.get_vector_of_rows().contains(&row2));
        assert!(table.get_vector_of_rows().contains(&row3));
    }

    #[test]
    fn test_get_vector_of_rows() {
        let mut table = create_table(vec!["id".to_string()], vec!["order".to_string()]);
        let row1 = create_row("111", "1", "data1");
        let row2 = create_row("111", "2", "data2");
        let row3 = create_row("111", "3", "data3");

        let _ = table.insert(row1.clone());
        let _ = table.insert(row2.clone());
        let _ = table.insert(row3.clone());

        let rows = table.get_vector_of_rows();
        assert_eq!(rows.len(), 3);
        assert!(rows.contains(&row1));
        assert!(rows.contains(&row2));
        assert!(rows.contains(&row3));
    }

    #[test]
    fn test_update() {
        let mut table = create_table(vec!["id".to_string()], vec!["order".to_string()]);
        let row1 = create_row("111", "1", "data1");
        let row2 = create_row("111", "2", "data2");
        let row3 = create_row("111", "3", "data3");

        let _ = table.insert(row1.clone());
        let _ = table.insert(row2.clone());
        let _ = table.insert(row3.clone());

        let mut values_to_update = HashMap::new();
        values_to_update.insert("data".to_string(), "updated_data".to_string());
        let condition = Expression::Comparison {
            left: Operand::Column("order".to_string()),
            operator: ">".to_string(),
            right: Operand::String("1".to_string()),
        };
        let result = table.update(values_to_update, &condition);

        assert!(result.is_ok());
        let updated_row1 = create_row("111", "1", "data1");
        let updated_row2 = create_row("111", "2", "updated_data");
        let updated_row3 = create_row("111", "3", "updated_data");

        assert!(table.get_vector_of_rows().contains(&updated_row1));
        assert!(table.get_vector_of_rows().contains(&updated_row2));
        assert!(table.get_vector_of_rows().contains(&updated_row3));
    }

    #[test]
    fn test_delete() {
        let mut table = create_table(vec!["id".to_string()], vec!["order".to_string()]);
        let row1 = create_row("111", "1", "data1");
        let row2 = create_row("111", "2", "data2");
        let row3 = create_row("111", "3", "data3");

        let _ = table.insert(row1.clone());
        let _ = table.insert(row2.clone());
        let _ = table.insert(row3.clone());

        let condition = Expression::Comparison {
            left: Operand::Column("order".to_string()),
            operator: ">".to_string(),
            right: Operand::String("1".to_string()),
        };
        let result = table.delete(&condition);

        assert!(result.is_ok());
        assert_eq!(table.get_vector_of_rows().len(), 1);
        assert!(table.get_vector_of_rows().contains(&row1));
    }

    #[test]
    fn test_contains_row() {
        let mut table = create_table(vec!["id".to_string()], vec!["order".to_string()]);
        let row1 = create_row("111", "1", "data1");
        let row2 = create_row("111", "2", "data2");
        let row3 = create_row("111", "3", "data3");

        let _ = table.insert(row1.clone());
        let _ = table.insert(row2.clone());
        let _ = table.insert(row3.clone());

        assert!(table.contains_row(&row1));
        assert!(table.contains_row(&row2));
        assert!(table.contains_row(&row3));
        assert!(!table.contains_row(&create_row("111", "4", "data4")));
    }
}
