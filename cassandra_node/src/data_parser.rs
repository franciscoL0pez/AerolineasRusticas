use std::{
    collections::HashMap,
    fs::{self, File},
    io::{BufRead, BufReader},
};

use crate::{encrypted_table::table::Table, node::GossipInformation};

/// Parsea una línea con comas en un vector de Strings.
pub fn parse_columns(line: &str) -> Result<Vec<String>, String> {
    Ok(line.split(",").map(|s| s.trim().to_string()).collect())
}

pub fn parse_columns_with_types(line: &str) -> Result<Vec<(String, String)>, String> {
    let mut columns_with_types: Vec<(String, String)> = vec![];
    let columns: Vec<String> = line.split(",").map(|s| s.trim().to_string()).collect();
    for column in columns {
        let column_parts: Vec<&str> = column.split(":").collect();
        if column_parts.len() != 2 {
            return Err(format!("Error: la columna '{}' no tiene un tipo asociado", column));
        }
        columns_with_types.push((column_parts[0].to_string(), column_parts[1].to_string()));
    }
    Ok(columns_with_types)
}

/// Parsea una linea que representa una fila convertiéndola en un hashmap.
/// Si la cantidad de valores en la fila no coincide con la cantidad de columnas, retorna un error.
pub fn parse_row(columns: &[String], line: &str) -> Result<HashMap<String, String>, String> {
    let values: Vec<&str> = line.split(",").collect();
    if values.len() != columns.len() {
        eprintln!(
            "Error: la cantidad de valores ({}) no coincide con la cantidad de columnas ({})",
            values.len(),
            columns.len()
        );
        return Err(format!(
            "Error: la cantidad de valores ({}) no coincide con la cantidad de columnas ({})",
            values.len(),
            columns.len()
        ));
    }
    let mut row_values: HashMap<String, String> = HashMap::new();
    for (i, value) in values.iter().enumerate() {
        if !value.is_empty() {
            row_values.insert(columns[i].to_string(), value.to_string());
        }
    }
    Ok(row_values)
}

// ------------------------  Recovery node data ------------------------

pub fn load_tables_path(node_id: &str) -> Result<Vec<String>, String> {
    let path = format!("./data/{}", node_id);

    // Leer el directorio
    let entries =
        fs::read_dir(&path).map_err(|e| format!("Error al leer el directorio {}: {}", path, e))?;

    let mut table_names = Vec::new();

    for entry in entries {
        let entry = entry.map_err(|e| {
            format!(
                "Error al procesar una entrada en el directorio {}: {}",
                path, e
            )
        })?;
        let path = entry.path();

        if path.is_file() {
            let file_name = path
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| format!("Nombre de archivo inválido en {:?}", path))?;
            if file_name.ends_with("keyspaces") || file_name.ends_with("gossip_table") {
                continue;
            }
            table_names.push(file_name.to_string());
        }
    }

    Ok(table_names)
}

pub fn load_keyspaces(node_id: &str) -> Result<Vec<(String, String, String)>, String> {
    let path = format!("./data/{}/keyspaces", node_id);

    let file =
        File::open(&path).map_err(|e| format!("Error al abrir el archivo {}: {}", path, e))?;
    let reader = BufReader::new(file);

    let mut keyspaces_data: Vec<(String, String, String)> = vec![];

    for (i, linea) in reader.lines().enumerate() {
        let line = linea.map_err(|e| format!("Error al leer la línea {}: {}", i + 1, e))?;

        let mut keyspace_data: (String, String, String) = ("".to_string(), "".to_string(), "".to_string());

        let keyspaces_parts: Vec<String> = line
            .split(',')
            .map(|s| s.trim().to_string()) // Eliminar espacios en blanco y convertir a String
            .collect();

        if keyspaces_parts.is_empty() || keyspaces_parts.len() != 3 {
            return Err("Error: la cantidad de datos del keyspace no es 3".to_string());
        }

        keyspace_data.0 = keyspaces_parts[0].to_string();
        keyspace_data.1 = keyspaces_parts[1].to_string();
        keyspace_data.2 = keyspaces_parts[2].to_string();

        keyspaces_data.push(keyspace_data);
    }

    Ok(keyspaces_data)
}

pub fn load_gossip_table(node_id: &str) -> Result<Vec<GossipInformation>, String> {
    let path = format!("./data/{}/gossip_table", node_id);

    let file =
        File::open(&path).map_err(|e| format!("Error al abrir el archivo {}: {}", path, e))?;
    let reader = BufReader::new(file);

    // Serialize from json
    let gossip_table: Vec<GossipInformation> = serde_json::from_reader(reader)
        .map_err(|e| format!("Error al leer el archivo {}: {}", path, e))?;

    Ok(gossip_table)
}

pub fn load_table(node_id: &str ,file_name: &str) -> Result<Table, String> {
    let path = format!("./data/{}/{}",node_id, file_name);

    let file =
        File::open(&path).map_err(|e| format!("Error al abrir el archivo {}: {}", path, e))?;
        
    let reader = BufReader::new(&file);

    let mut partition_key_columns: Vec<String> = vec![];
    let mut clustering_key_columns: Vec<String> = vec![];
    let mut columns_with_types: Vec<(String, String)> = vec![];
    let mut columns: Vec<String> = vec![];

    for (i, linea) in reader.lines().enumerate() {
        let line = linea.map_err(|e| format!("Error al leer la línea {}: {}", i + 1, e))?;
        if i == 0 {
            // leer partition keys
            partition_key_columns = parse_columns(&line)?;
            continue;
        }
        if i == 1 {
            // leer clustering keys
            clustering_key_columns = parse_columns(&line)?;
            continue;
        }
        if i == 2 {
            // leer columnas con sus tipos
            columns_with_types = parse_columns_with_types(&line)?;
            continue;
        }
        if i == 3 {
            // leer columnas sin tipos
            columns = parse_columns(&line)?;
            continue;
        }
        break;
    }

    // table name is the file name without .csv
    let table_name = file_name.split(".csv").collect::<Vec<&str>>()[0];

    let mut table = Table::new(
        table_name.to_string(),
        partition_key_columns,
        clustering_key_columns,
        columns_with_types,
    );
    
    let file =
        File::open(&path).map_err(|e| format!("Error al abrir el archivo {}: {}", path, e))?;
        
    let reader = BufReader::new(&file);

    for (i, linea) in reader.lines().enumerate() {
        let line = linea.map_err(|e| format!("Error al leer la línea {}: {}", i + 1, e))?;
        if i == 0 || i == 1 || i == 2 || i == 3 {
            continue;
        }

        let row = parse_row(&columns, &line)?;
        if table.insert(row).is_err() {
            eprintln!("Error al insertar una fila en la tabla");
            return Err("Error al insertar una fila en la tabla".to_string());
        }
    }

    Ok(table)
}



