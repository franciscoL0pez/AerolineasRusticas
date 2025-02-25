use crate::consistency::Consistency;
use crate::consistent_hashing::ConsistentHash;
use crate::data_parser::{load_keyspaces, load_tables_path, load_gossip_table};
use crate::encrypted_table::table::Table;
use crate::encrypted_table::EncryptedTable;
use crate::internal_protocol::InternalMessage;
use crate::log::Logger;
use crate::query_parser::expression::{extract_value_supposing_column_equals_value, Expression};
use crate::query_parser::{parse_instruction, ParsedQuery};
use crate::replication_strategy::ReplicationStrategy;
use crate::query_builder::{insert_message_from_row_and_tablename, create_keyspace_query, create_table_query, add_timestamp_to_insert_message, add_timestamp_to_update_message};
use chrono::{NaiveDateTime, TimeZone, Utc};
use common::frame::messages::error::ErrorCode;
use common::frame::messages::query::Query;
use common::frame::messages::query_result::QueryResult;
use rand::{rng, Rng};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::f64::consts::E;
use std::net::TcpStream;
use std::sync::{mpsc, Arc, RwLock};
use std::{fs, vec};

//Comunicacion interna entre nodos
//Remplazamos el uso de serde con este mini protocolo interno
// pub enum InternalMessage {
//     Gossip { // 0
//         opcode: u8,
//         body: String,
//     },
//     Query { // 1
//         opcode: u8,
//         body: String,
//         keyspace_name: String,
//         consistency_level: String,
//     }
// }

/// This struct represents information about a node in the gossip protocol.
///
/// It is used to exchange state and status information during the gossip process.
///
/// # Fields
/// - `node_id`: A unique identifier for the node.
/// - `ip`: The IP address of the node.
/// - `port_native_protocol`: Port for the native client protocol.
/// - `port_gossip_query`: Port for internal gossip communication between nodes.
/// - `last_heartbeat`: A timestamp indicating the node's last known activity.
/// - `status`: The status of the node.
///
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct GossipInformation {
    pub node_id: String,
    pub ip: String,
    pub port_native_protocol: String,
    pub port_gossip_query: String,
    pub last_heartbeat: i64, // timestamp
    pub status: String,
}

/// Represents the node in our distributed system
///
///  # Fields
/// - `id`: A unique identifier for this node, typically used to distinguish it from other nodes.
/// - `ip`: The IP address of the node.
/// - `port_native_protocol`: Port for the native client protocol used to handle queries.
/// - `port_gossip_query`: Port for the internal gossip communication between nodes.
/// - `gossip_table`: A shared table containing metadata about other nodes in the cluster,
///    including their status, heartbeat, and other gossip protocol information.
/// - `consistent_hash`: A consistent hash structure used to distribute data among nodes in a
///    predictable and load-balanced manner.
/// - `data`: Stores the data managed by this node. The keys are table names in the format
///    `keyspace_name.table_name`. Note: The keyspace name must not contain periods (`.`).
/// - `keyspaces`: Configuration for keyspaces in the node. Maps keyspace names to their
///    replication strategies.
/// - `hints`: A shared structure for holding unacknowledged write hints for eventual consistency
///    during node outages. Keys represent nodes for which the hints are maintained.
/// - `logger`: A logger instance for tracking node activity and debugging.
///
#[derive(Clone, Debug)]
pub struct Node {
    id: String,
    ip: String,
    port_native_protocol: u16,
    port_gossip_query: u16,

    pub gossip_table: Arc<RwLock<Vec<GossipInformation>>>,
    consistent_hash: ConsistentHash,
    data: Arc<RwLock<HashMap<String, EncryptedTable>>>,
    keyspaces: Arc<RwLock<HashMap<String, ReplicationStrategy>>>,
    hints: Arc<RwLock<HashMap<String, Vec<InternalMessage>>>>,
    logger: Logger,
}

impl Node {
    /// Creates a new instance of a `Node`.
    ///
    /// #Parameters
    /// - `id`: A unique identifier for this node.
    /// - `ip`: The IP address of the node as a string.
    /// - `port_native_protocol`: The port number for the native client protocol.
    /// - `port_gossip_query`: The port number used for the gossip communication protocol.
    ///
    /// # Returns
    /// A fully initialized `Node` with default values for its components.
    ///
    pub fn new(id: &str, ip: &str, port_native_protocol: u16, port_gossip_query: u16) -> Self {
        let gossip_information = GossipInformation {
            node_id: id.to_string(),
            ip: ip.to_string(),
            port_native_protocol: port_native_protocol.to_string(),
            port_gossip_query: port_gossip_query.to_string(),
            last_heartbeat: Utc::now().timestamp(),
            status: "Live".to_string(),
        };

        let gossip_table = vec![gossip_information];

        let node = Node {
            id: id.to_string(),
            ip: ip.to_string(),
            port_native_protocol,
            port_gossip_query,
            gossip_table: Arc::new(RwLock::new(gossip_table)),
            consistent_hash: ConsistentHash::new(),
            data: Arc::new(RwLock::new(HashMap::new())),
            keyspaces: Arc::new(RwLock::new(HashMap::new())),
            hints: Arc::new(RwLock::new(HashMap::new())),
            logger: Logger::new(id),
        };
        node.load_data();

        node
    }

    // ------------------------ Logger ------------------------

    /// Returns a clone of the logger associated with this node.
    ///
    /// # Returns
    /// A `Logger` instance cloned from the internal logger field.
    ///
    pub fn get_logger(&self) -> Logger {
        self.logger.clone()
    }

    // ------------------------ Debug ------------------------

    /// Displays the contents of all tables stored in the node.
    ///
    /// The method reads the `data` field, which contains the node's stored tables,
    /// and calls the `show` method on each `EncryptedTable` to display its contents.
    ///
    pub fn show(&self) {
        let data = match self.data.read() {
            Ok(data) => data.clone(),

            Err(e) => {
                eprintln!("Error reading data: {}", e);
                return;
            }
        };

        for table in data.values() {
            table.show();
        }
    }

    /// Get the specific table from the node's data storage.
    ///
    /// #Parameters
    /// - `keyspace_name`: The name of the keyspace containing the table.
    /// - `table_name`: The name of the table within the keyspace.
    ///
    /// #Returns
    /// An `Option` containing the requested table if it exists, or `None` if it does not.
    ///
    pub fn get_table(&self, keyspace_name: &str, table_name: &str) -> Option<Table> {
        let data = match self.data.read() {
            Ok(data) => data.clone(),

            Err(_) => {
                return None;
            }
        };

        data.get(format!("{}.{}", keyspace_name, table_name).as_str())
            .map(|table| table.get_table())
    }

    // ------------------------ Getter ------------------------

    /// Returns the port number used for the gossip query protocol.
    ///
    pub fn get_port_gossip_query(&self) -> u16 {
        self.port_gossip_query
    }

    /// Returns the port number used for the native protocol.
    ///
    pub fn get_port_native_protocol(&self) -> u16 {
        self.port_native_protocol
    }

    /// Retrieves the gossip table for this node.
    ///
    /// # Returns
    /// Ok(Vec<GossipInformation>) on success, or a descriptive Err(String) on failure.
    ///
    pub fn get_gossip_table(&self) -> Result<Vec<GossipInformation>, String> {
        match self.gossip_table.read() {
            Ok(gossip_table) => Ok(gossip_table.clone()),
            _ => Err("Failed locking gossip table".to_string()),
        }
    }

    fn get_keyspaces(&self) -> Result<HashMap<String, ReplicationStrategy>, String> {
        match self.keyspaces.read() {
            Ok(keyspaces) => Ok(keyspaces.clone()),
            Err(_) => Err("Failed locking keyspaces".to_string()),
        }
    }

    fn get_data(&self) -> Result<HashMap<String, EncryptedTable>, String> {
        match self.data.read() {
            Ok(data) => Ok(data.clone()),
            Err(_) => Err("Failed locking data".to_string()),
        }
    }

    /// Retrieves the IP address of this node.
    ///
    pub fn get_ip(&self) -> &str {
        &self.ip
    }
    /// Retrieves the unique identifier (ID) of this node.
    ///
    pub fn get_id(&self) -> &str {
        &self.id
    }

    // ------------------------ Gossip ------------------------

    /// Update the local gossip table with the information received from another node.
    /// If a new node is detected, the method will reassign partitions that don't belong anymore to the current node.
    ///
    /// # Parameters
    /// - `received_gossip_table`: A vector of `GossipInformation` instances containing the information
    ///   to be added to the local gossip table.
    ///
    pub fn update_gossip_table(&self, received_gossip_table: &[GossipInformation]) {
        let mut local_gossip_table = match self.gossip_table.write() {
            Ok(gossip_table) => {
                //println!("Entre a bloquear el gossip table");
                gossip_table
            }

            _ => {
                return;
            }
        };
        let mut new_node_detected = false;
        let mut new_nodes_info = vec![];
        for gossip_info in received_gossip_table.iter().cloned() {
            let mut found = false;
            for local_gossip_info in local_gossip_table.iter_mut() {
                if local_gossip_info.node_id == gossip_info.node_id {
                    found = true;
                    if local_gossip_info.last_heartbeat < gossip_info.last_heartbeat {
                        if local_gossip_info.status == "Dead" && gossip_info.status == "Live" {
                            // Si el nodo estaba muerto y ahora esta vivo, enviamos hints
                            let _ = self.logger.log(
                                format!(
                                    "Node {} is marked live again, sending hints",
                                    gossip_info.node_id
                                )
                                .as_str(),
                            );
                            // mando a un thread para que mande los hints
                            let self_arc = Arc::new(self.clone());
                            let gossip_info_cloned = gossip_info.clone();
                            std::thread::spawn(move || {
                                self_arc.send_hints(
                                    gossip_info_cloned.node_id,
                                    gossip_info_cloned.ip,
                                    gossip_info_cloned.port_gossip_query,
                                );
                            });
                        }
                        local_gossip_info.last_heartbeat = gossip_info.last_heartbeat;
                        local_gossip_info.status = gossip_info.status.clone();
                    }
                    break;
                }
            }
            if !found {
                local_gossip_table.push(gossip_info.clone());
                new_node_detected = true;
                new_nodes_info.push(gossip_info.clone());
            }
        }
        local_gossip_table.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        self.flush_gossip_table(local_gossip_table.to_vec());

        std::mem::drop(local_gossip_table);

        if new_node_detected {
            let _ = self.logger.log(
                "New node detected, reassigning data...."
            );
            // Reassign data
            self.reassign_data(new_nodes_info);
        }
    }

    /// Flushes the gossip table to disk so it can be retrieved after a node restart.
    fn flush_gossip_table(&self, local_gossip_table: Vec<GossipInformation>) {
        // Write to disk every information of the gossip table
        let dir = format!("./data/{}", self.id);
        let file = format!("{}/gossip_table", dir);
        if let Err(e) = fs::create_dir_all(&dir) {
            eprintln!("Error creating directory: {}", e);
        }
        let json = match serde_json::to_string(&local_gossip_table) {
            Ok(json) => json,
            Err(e) => {
                eprintln!("Error serializing gossip table: {}", e);
                return;
            }
        };
        if let Err(e) = fs::write(&file, json) {
            eprintln!("Error writing gossip table to disk: {}", e);
        }
    }

    fn reassign_data(&self, new_nodes: Vec<GossipInformation>) {
        let keyspaces = match self.get_keyspaces() {
            Ok(keyspaces) => keyspaces,
            Err(_) => {
                eprintln!("Error getting keyspaces");
                return;
            }
        };
        let data = match self.get_data() {
            Ok(data) => data,
            Err(_) => {
                eprintln!("Error getting data");
                return;
            }
        };

        // First, create send the create keyspace and create table messages to the new nodes
        for (keyspace_name, replication_strategy) in keyspaces.clone() {
            let body = create_keyspace_query(&keyspace_name, replication_strategy);
            let create_keyspace_message = InternalMessage::Query {
                opcode: 0,
                body,
                keyspace_name: "".to_string(),
            };
            for node_info in &new_nodes {
                let _ = send_internal_message_and_return_response(&create_keyspace_message, &node_info.ip, &node_info.port_gossip_query);
            }
            for (_, table) in &data {
                let body = create_table_query(&table.get_table());
                let create_table_message = InternalMessage::Query {
                    opcode: 1,
                    body,
                    keyspace_name: "".to_string(),
                };
                for node_info in &new_nodes {
                    let _ = send_internal_message_and_return_response(&create_table_message, &node_info.ip, &node_info.port_gossip_query);
                }
            }
        }
        
        let local_gossip_table = if let Ok(gossip_table) = self.get_gossip_table() {
            gossip_table
        } else {
            return;
        };

        let mut partitions_to_reassign: Vec<Vec<String>> = vec![];
        // Check for every partition if it is still in one of the correct nodes, if not, send it to the correct node
        for (_, table) in data.iter() {
            let keyspace_name = table.get_keyspace_name();
            let replication_strategy = if let Some(replication_strategy) = keyspaces.get(&keyspace_name) {
                replication_strategy.clone()
            } else {
                continue;
            };
            for (partition_keys, _) in table.get_partitions() {
                // Check the nodes that should have the partition with the replication strategy
                let nodes = replication_strategy.get_replica_nodes(
                    &partition_keys,
                    &local_gossip_table,
                    &self.consistent_hash,
                );
                if !nodes.contains(&self.id) && !partitions_to_reassign.contains(&partition_keys) {
                    partitions_to_reassign.push(partition_keys.clone());
                }
            }
        }

        // Identify non-corresponding partitions and send rows from them to the correct nodes
        for (table_name_with_keyspace, table) in data.iter() {
            let keyspace_name = table_name_with_keyspace.split('.').collect::<Vec<&str>>()[0];
            let table_name = table_name_with_keyspace.split('.').collect::<Vec<&str>>()[1];
            for partition_keys in partitions_to_reassign.clone() {
                let replication_strategy =
                if let Some(replication_strategy) = (&keyspaces).get(keyspace_name) {
                    replication_strategy.clone()
                } else {
                    continue;
                };
                let replica_nodes = replication_strategy.get_replica_nodes(
                    &partition_keys,
                    &local_gossip_table,
                    &self.consistent_hash,
                );
                let rows_to_send = table.get_rows_from_partition(&partition_keys);
                for row in rows_to_send {
                    let body = insert_message_from_row_and_tablename(&row, table_name);
                    let internal_message = InternalMessage::Query {
                        opcode: 2,
                        body,
                        keyspace_name: keyspace_name.to_string(),
                    };
                    for node_id in &replica_nodes {
                        for new_node_info in &new_nodes {
                            if node_id == &new_node_info.node_id {
                                if let Ok(_) = send_internal_message_and_return_response(&internal_message, &new_node_info.ip, &new_node_info.port_gossip_query) {
                                    let _ = self.logger.log(
                                        format!("Data reassigned from {} to {}", self.id, node_id).as_str(),
                                    );
                                } else {
                                    let _ = self.logger.log(
                                        format!("Error reassigning data to {}", node_id).as_str(),
                                    );
                                }
                            }
                            break;
                        }
                    }
                }
            }
        }

        // Delete the partitions from the node
        let mut data = match self.data.write() {
            Ok(data) => data,
            Err(_) => {
                eprintln!("Error getting data for write");
                return;
            }
        };
        for (_, table) in data.iter_mut() {
            for partition_keys in partitions_to_reassign.clone() {
                if let Err(_) = table.delete_partition(&partition_keys) {
                    eprintln!("Partition already deleted from table");
                }
            }
        }

    }


    /// Sends the pending hints to the specified node.
    ///
    /// # Parameters
    /// - `node_id`: The unique identifier of the node to which the hints will be sent.
    /// - `node_ip`: The IP address of the node to which the hints will be sent.
    /// - `node_port`: The port number of the node to which the hints will be sent.
    ///
    fn send_hints(&self, node_id: String, node_ip: String, node_port: String) {
        let mut hints = match self.hints.write() {
            Ok(hints) => hints,
            _ => {
                return;
            }
        };

        let hints_to_send = match hints.get_mut(&node_id) {
            Some(hints_to_send) => hints_to_send,
            None => {
                return;
            }
        };

        let mut hints_successful: Vec<InternalMessage> = vec![];

        for hint in hints_to_send.iter() {
            let destination = format!("{}:{}", node_ip, node_port);
            if let Ok(mut stream) = TcpStream::connect(&destination) {
                if let Err(e) = hint.write_to_stream(&mut stream) {
                    eprintln!("Error writing to stream: {}", e);
                } else {
                    hints_successful.push(hint.clone());
                }
            } else {
                eprintln!(
                    "Error connecting from node {} to {:?}",
                    self.id, &destination
                );
            }
        }

        // Borro todos los hints que fueron enviados del vector hints_to_send
        for hint in hints_successful {
            if let Some(index) = hints_to_send.iter().position(|x| *x == hint) {
                hints_to_send.remove(index);
            }
        }
    }

    /// Calculates the value of φ (phi) given a lambda (λ) parameter and elapsed time.
    ///
    /// #Parameters
    /// - 'lambda': The rate of event occurrence
    /// - 'elapsed_time': The time elapsed
    ///
    /// #Returns
    ///- Returns the calculated value of φ
    ///
    pub fn calcular_phi(lambda: f64, tiempo_transcurrido: f64) -> f64 {
        let probabilidad = 1.0 - (E.powf(-lambda * tiempo_transcurrido));
        -probabilidad.log10()
    }

    /// Sends periodic gossip messages to other nodes in the system and updates gossip tables.
    ///
    /// #Parameters
    /// - 'interval': The interval in milliseconds at which gossip messages are sent
    ///
    pub fn gossip(&self, interval: u64) {
        // println!("[{}] Attempting to send gossip", Utc::now().format("%Y-%m-%d %H:%M:%S"));
        let mut local_gossip_table = match self.gossip_table.write() {
            Ok(gossip_table) => gossip_table,

            _ => {
                return;
            }
        };

        if local_gossip_table.len() == 1 {
            return;
        }
        // P(t-T) = 1-e^(-λ(t-T))
        // Phi = -log(P(t-T))

        // λ es la tasa media de mensajes gossip por segundo
        // t-T es el tiempo del ultimo mensaje recibido
        //
        // Para todos los nodos, si el tiempo del ultimo mensaje recibido es mayor a threshold, se lo marca como caido
        // Usamos una distribucion exponencial para calcula

        // 1. Calcular el tiempo transcurrido desde el ultimo mensaje recibido

        let tiempo_actual = Utc::now().timestamp();

        for gossip_info in local_gossip_table.iter_mut() {
            if gossip_info.node_id == self.id {
                gossip_info.status = "Live".to_string();
                gossip_info.last_heartbeat = tiempo_actual;
                continue;
            }

            let tiempo_transcurrido = tiempo_actual - gossip_info.last_heartbeat;
            let interval_in_seconds = interval as f64 / 1000.0;
            let phi = Node::calcular_phi(interval_in_seconds, tiempo_transcurrido as f64);
            if phi < 0.0000000015 {
                let _ = self.logger.log(
                    format!(
                        "Node {} is marked dead, {} seconds has passed since its last heartbeat",
                        gossip_info.node_id, tiempo_transcurrido
                    )
                    .as_str(),
                );
                if gossip_info.status == "Live" {
                    gossip_info.status = "Dead".to_string();
                }
            }
        }

        let mut rng = rng();

        let mut gossip_table_cloned = local_gossip_table.clone();
        let mut random_node_info = GossipInformation {
            node_id: "".to_string(),
            ip: "".to_string(),
            port_native_protocol: "".to_string(),
            port_gossip_query: "".to_string(),
            last_heartbeat: 0,
            status: "".to_string(),
        };

        for _ in 0..local_gossip_table.len() {
            let random_index = rng.random_range(0..gossip_table_cloned.len());
            let node_info = gossip_table_cloned[random_index].clone();

            if node_info.node_id != self.id && node_info.status == "Live" {
                random_node_info = node_info;
                break;
            } else {
                gossip_table_cloned.remove(random_index);
            }
        }

        if random_node_info.node_id.is_empty() {
            println!("No node alive to gossip with");
            return;
        }

        let destination = format!(
            "{}:{}",
            random_node_info.ip, random_node_info.port_gossip_query
        );

        match serde_json::to_string(&*local_gossip_table) {
            Ok(json) => {
                let internal_message = InternalMessage::Gossip {
                    opcode: 0,
                    body: json,
                };
                if let Ok(mut stream) = TcpStream::connect(&destination) {
                    if let Err(e) = internal_message.write_to_stream(&mut stream) {
                        eprintln!("Error sending gossip: {}", e);
                    }
                } else {
                    eprintln!(
                        "Error connecting from {} to node {:?}",
                        self.id, &destination
                    );
                }
            }
            Err(e) => {
                eprintln!("Error serializing gossip table: {}", e);
            }
        }
    }

    // ------------------------ Direct Keyspace Management ------------------------
    // Se utilizan cuando se quiere manejar keyspaces directamente

    /// Creates a new keyspace with the specified replication strategy and replication factor.
    ///
    /// # Parameters
    /// - `keyspace_name`: The name of the keyspace to be created.
    /// - `replication_strategy`: The replication strategy to be used for the keyspace.
    /// - `replication_factor`: The replication factor to be used for the keyspace.
    ///
    /// # Returns
    /// An `Ok(())` value if the keyspace was created successfully, or an `Err(String)` with an error message if the operation failed.
    fn create_keyspace(
        &self,
        keyspace_name: &str,
        replication_strategy: &str,
        replication_factor: &str,
    ) -> Result<(), String> {
        let mut keyspaces = match self.keyspaces.write() {
            Ok(keyspaces) => keyspaces,

            Err(e) => {
                return Err(format!("Error locking keyspaces: {}", e));
            }
        };

        match replication_strategy {
            "SimpleStrategy" => {
                keyspaces.insert(
                    keyspace_name.to_string(),
                    ReplicationStrategy::new_simple(replication_factor.to_string()),
                );
            }
            _ => {
                return Err("Invalid replication strategy".to_string());
            }
        }
        Ok(())
    }

    /// Check if the keyspace exist .
    ///
    /// # Parameters
    /// - `keyspace_name`: The name of the keyspace to be checked.
    ///
    /// # Returns
    /// A boolean value indicating whether the keyspace exists.
    fn keyspace_exists(&self, keyspace_name: &str) -> bool {
        let keyspaces = match self.keyspaces.read() {
            Ok(keyspaces) => keyspaces.clone(),

            Err(_) => {
                return false;
            }
        };

        keyspaces.contains_key(keyspace_name)
    }

    // ------------------------ Direct Table Management ------------------------
    // Se utilizan cuando se quiere manejar data directamente

    /// Creates a new encrypted table with the specified parameters.
    ///
    /// # Parameters
    /// - `keyspace_name`: The name of the keyspace to which the table belongs.
    /// - `table_name`: The name of the table to be created.
    /// - `partition_key_columns`: A vector containing the names of the columns to be used as partition keys.
    /// - `clustering_key_columns`: A vector containing the names of the columns to be used as clustering keys.
    /// - `columns`: A vector containing tuples with the name and type of each column in the table.
    fn create_encrypted_table(
        &self,
        keyspace_name: &str,
        table_name: &str,
        partition_key_columns: Vec<String>,
        clustering_key_columns: Vec<String>,
        columns: Vec<(String, String)>,
    ) {
        let table = Table::new(
            format!("{}.{}", keyspace_name, table_name),
            partition_key_columns,
            clustering_key_columns,
            columns,
        );

        let mut data = match self.data.write() {
            Ok(data) => data,

            Err(_) => {
                return;
            }
        };

        let encrypted_table = EncryptedTable::new(table);
        data.insert(format!("{}.{}", keyspace_name, table_name), encrypted_table);
    }

    /// Inserts a new row into the specified table
    ///
    /// # Parameters
    /// - `keyspace_name`: The name of the keyspace containing the table.
    /// - `table_name`: The name of the table in which the row will be inserted.
    /// - `values`: A hashmap containing the column names and values for the new row.
    ///
    /// # Returns
    /// An `Ok(())` value if the row was inserted successfully, or an `Err(String)` with an error message if the operation failed.

    pub fn insert_row(
        &self,
        keyspace_name: &str,
        table_name: &str,
        values: HashMap<String, String>,
    ) -> Result<(), String> {
        let mut data = match self.data.write() {
            Ok(data) => {
                // println!("Entre a bloquear data");
                data
            }

            Err(_) => {
                return Err("Error locking data".to_string());
            }
        };

        if let Some(table) = data.get_mut(&format!("{}.{}", keyspace_name, table_name)) {
            table.insert(values)
        } else {
            Err("Table not found".to_string())
        }
    }
    /// Update a row in the specified table
    ///
    /// # Parameters
    /// - `keyspace_name`: The name of the keyspace containing the table.
    /// - `table_name`: The name of the table in which the row will be updated.
    /// - `values_to_update`: A hashmap containing the column names and values to be updated.
    /// - `condition`: An `Expression` representing the condition that must be met for the row to be updated.
    ///
    /// # Returns
    /// An `Ok(())` value if the row was updated successfully, or an `Err(String)` with an error message if the operation failed.
    pub fn update_row(
        &self,
        keyspace_name: &str,
        table_name: &str,
        values_to_update: HashMap<String, String>,
        condition: &Expression,
    ) -> Result<(), String> {
        let mut data = match self.data.write() {
            Ok(data) => {
                // println!("Entre a bloquear data");
                data
            }

            Err(_) => {
                return Err("Error locking data".to_string());
            }
        };

        if let Some(table) = data.get_mut(&format!("{}.{}", keyspace_name, table_name)) {
            table.update(values_to_update, condition)
        } else {
            Err("Table not found".to_string())
        }
    }

    /// Deletes a row in the specified table
    ///
    /// # Parameters
    /// - `keyspace_name`: The name of the keyspace containing the table.
    /// - `table_name`: The name of the table in which the row will be deleted.
    /// - `condition`: An `Expression` representing the condition that must be met for the row to be deleted.
    ///
    /// # Returns
    /// An `Ok(())` value if the row was deleted successfully, or an `Err(String)` with an error message if the operation failed.
    pub fn delete_row(
        &self,
        keyspace_name: &str,
        table_name: &str,
        condition: &Expression,
    ) -> Result<(), String> {
        let mut data = match self.data.write() {
            Ok(data) => {
                // println!("Entre a bloquear data");
                data
            }

            Err(_) => {
                return Err("Error locking data".to_string());
            }
        };

        if let Some(table) = data.get_mut(&format!("{}.{}", keyspace_name, table_name)) {
            table.delete(condition)
        } else {
            Err("Table not found".to_string())
        }
    }

    // ------------------------  Methods without native protocole to test ------------------------//

    /// Resends a parsed query as an internal message to the corresponding nodes.
    ///
    /// # Parameters
    /// - `query`: The parsed query to be resent.
    /// - `keyspace`: The name of the keyspace to which the query belongs.
    ///
    /// # Returns
    /// A `String` containing the response from the first node to respond, or an error message if the operation failed.

    pub fn resend_query_as_internal_message_mock_not_native(
        &self,
        query_str: &str,
        keyspace_name: &str,
    ) -> Result<String, String> {
        let query_parsed = if let Ok(parsed_queries) = parse_instruction(query_str) {
            parsed_queries
        } else {
            return Err("Error parsing query".to_string());
        };

        match &query_parsed {
            ParsedQuery::CreateKeyspace { .. } => {
                let to_send = InternalMessage::Query {
                    opcode: 0,
                    body: query_str.to_string(),
                    keyspace_name: "not_necessary".to_string(),
                };
                let nodes_to_resend_query = self.get_all_nodes();
                let mut responses = vec![];
                for node_id in &nodes_to_resend_query {
                    responses.push(self.resend(&to_send, node_id));
                }
                responses[0].clone()
            }
            ParsedQuery::CreateTable { .. } => {
                let to_send = InternalMessage::Query {
                    opcode: 1,
                    body: query_str.to_string(),
                    keyspace_name: keyspace_name.to_string(),
                };
                let nodes_to_resend_query = self.get_all_nodes();
                let mut responses = vec![];
                for node_id in &nodes_to_resend_query {
                    responses.push(self.resend(&to_send, node_id));
                }
                responses[0].clone()
            }
            ParsedQuery::Insert {
                table_name,
                rows_to_insert,
                ..
            } => {
                let to_send = InternalMessage::Query {
                    opcode: 2,
                    body: query_str.to_string(),
                    keyspace_name: keyspace_name.to_string(),
                };

                let nodes_to_resend_query =
                    self.get_nodes_for_insert(keyspace_name, table_name, &rows_to_insert[0]);
                let mut responses = vec![];
                for node_id in &nodes_to_resend_query {
                    responses.push(self.resend(&to_send, node_id));
                }
                responses[0].clone()
            }
            ParsedQuery::Select { condition, .. } => {
                let to_send = InternalMessage::Query {
                    opcode: 3,
                    body: query_str.to_string(),
                    keyspace_name: keyspace_name.to_string(),
                };

                let nodes_to_resend_query = self.get_nodes_for_condition(keyspace_name, condition);

                let mut responses = vec![];
                for node_id in &nodes_to_resend_query {
                    responses.push(self.resend(&to_send, node_id));
                }

                responses[0].clone()
            }
            ParsedQuery::Update { .. } => {
                let to_send = InternalMessage::Query {
                    opcode: 4,
                    body: query_str.to_string(),
                    keyspace_name: keyspace_name.to_string(),
                };
                let nodes_to_resend_query = self.get_all_nodes();
                let mut responses = vec![];
                for node_id in &nodes_to_resend_query {
                    responses.push(self.resend(&to_send, node_id));
                }
                responses[0].clone()
            }
            ParsedQuery::Delete { .. } => {
                let to_send = InternalMessage::Query {
                    opcode: 5,
                    body: query_str.to_string(),
                    keyspace_name: keyspace_name.to_string(),
                };
                let nodes_to_resend_query = self.get_all_nodes();
                let mut responses = vec![];
                for node_id in &nodes_to_resend_query {
                    responses.push(self.resend(&to_send, node_id));
                }
                responses[0].clone()
            }

            ParsedQuery::UseKeyspace { keyspace_name } => {
                if self.keyspace_exists(keyspace_name) {
                    Ok("Keyspace changed".to_string())
                } else {
                    Err("Keyspace not found".to_string())
                }
            }
        }
    }

    /// Resolves inconsistencies between responses based on the timestamp,
    ///
    /// # Parameters
    /// - `responses`: A vector of responses from different nodes.
    /// - `keyspace_name`: The name of the keyspace to which the query belongs.
    /// - `table_name`: The name of the table to which the query belongs.
    ///
    /// # Returns
    /// A `String` containing the response with the most recent timestamp.
    /// in case of error returns a string with the error message.

    pub fn read_repair(
        &self,
        responses: &[String],
        keyspace_name: &str,
        table_name: &str,
    ) -> String {
        let mut last_timestamp = 0;
        let mut last_index = 0;
        let mut found_mismatch = false;

        for (i, response) in responses.iter().enumerate() {
            let rows: Vec<HashMap<String, String>> = match serde_json::from_str(response) {
                Ok(rows) => rows,
                Err(e) => {
                    eprintln!("Error deserializing response: {}", e);
                    continue;
                }
            };

            for row in rows {
                if let Some(timestamp_str) = row.get("_timestamp") {
                    let naive_dt =
                        match NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S") {
                            Ok(dt) => dt,
                            Err(_) => {
                                eprintln!("Error parsing timestamp");
                                return "Error parsing timestamp".to_string();
                            }
                        };

                    let timestamp = Utc.from_utc_datetime(&naive_dt).timestamp();
                    if timestamp > last_timestamp {
                        last_timestamp = timestamp;
                        last_index = i;

                        found_mismatch = true;
                    }
                }
            }
        }

        if found_mismatch {
            let rows: Vec<HashMap<String, String>> =
                match serde_json::from_str(&responses[last_index]) {
                    Ok(rows) => rows,
                    Err(e) => {
                        eprintln!("Error deserializing row: {}", e);
                        return "Error deserializing row".to_string();
                    }
                };

            if let Some(row) = rows.first() {
                let values = row.clone();
                let nodes_to_resend_query =
                    self.get_nodes_for_insert(keyspace_name, table_name, &values);
                let body = generate_insert_cql(table_name, values);
                let to_send = InternalMessage::Query {
                    opcode: 2,
                    body: body.clone(),
                    keyspace_name: keyspace_name.to_string(),
                };

                let self_arc = Arc::new(self.clone());
                let _ = self.logger.log(
                    format!("Read repair needed on {:?}", nodes_to_resend_query.clone()).as_str(),
                );
                for node_id in nodes_to_resend_query.clone() {
                    let to_send = to_send.clone();
                    let self_arc = Arc::clone(&self_arc);
                    std::thread::spawn(move || {
                        // println!("nodo a enviar: {}", &node_id);
                        let _ = self_arc.resend(&to_send, &node_id);
                    });
                }
            }
        }

        responses[last_index].clone()
    }

    // ------------------------  Resend Query ------------------------//

    /// Resends a query as an internal message to the corresponding nodes.
    ///
    /// # Parameters
    ///  `query`: A `Query` containing the string representation of the query to be executed.
    ///  `current_keyspace`: keyspace name indicating the context under which the query should be run.
    ///
    /// # Returns
    /// A `QueryResult` containing the response from the first node to respond, or an `ErrorCode` if the operation failed.

    pub fn resend_query_as_internal_message(
        &self,
        query: Query,
        current_keyspace: Option<String>,
    ) -> Result<QueryResult, ErrorCode> {
        let query_str = query.query_string;
        let _ = self
            .logger
            .log(format!("Received query from client: {}", query_str).as_str());
        let consistency_level = Consistency::from_consistency_level(query.consistency_level);

        let local_gossip_table = match self.gossip_table.read() {
            Ok(gossip_table) => gossip_table.clone(),
            Err(_) => {
                return Err(ErrorCode::UnavailableException);
            }
        };

        let Ok(query_parsed) = parse_instruction(&query_str) else {
            eprintln!("Error parsing query");
            return Err(ErrorCode::SyntaxError);
        };

        if current_keyspace.is_none() {
            if let ParsedQuery::CreateKeyspace { .. } | ParsedQuery::UseKeyspace { .. } =
                query_parsed
            {
                // do nothing
            } else {
                eprintln!("No keyspace set");
                return Err(ErrorCode::Invalid);
            }
        }
        let mut keyspace_name = "".to_string();
        if let Some(keyspace_name_as_string) = current_keyspace {
            keyspace_name = keyspace_name_as_string;
        }

        match &query_parsed {
            ParsedQuery::CreateKeyspace { .. } => {
                let to_send = InternalMessage::Query {
                    opcode: 0,
                    body: query_str.to_string(),
                    keyspace_name: "not_neccessary".to_string(),
                };
                let nodes_to_resend_query = self.get_all_nodes();
                let _ = self
                    .logger
                    .log(format!("Nodes to resend query: {:?}", nodes_to_resend_query).as_str());
                let mut responses = vec![];
                let self_cloned = Arc::new(self.clone());
                for node_id in &nodes_to_resend_query {
                    responses.push(self_cloned.resend(&to_send, node_id));
                }

                let final_response =
                    match responses.iter().find(|&response| response.is_ok()).cloned() {
                        Some(response) => response,
                        None => Err("None of the responses were successful".to_string()),
                    };

                let _ = self
                    .logger
                    .log(format!("Create keyspace response: {:?}", final_response).as_str());

                match final_response {
                    Ok(response) => Ok(QueryResult::SchemaChange {
                        change_type: response,
                        target: Default::default(),
                        options: Default::default(),
                    }),
                    Err(e) => {
                        eprintln!("{}", e);
                        Err(ErrorCode::Invalid)
                    }
                }
            }
            ParsedQuery::CreateTable { .. } => {
                let to_send = InternalMessage::Query {
                    opcode: 1,
                    body: query_str.to_string(),
                    keyspace_name,
                };
                let nodes_to_resend_query = self.get_all_nodes();
                let _ = self
                    .logger
                    .log(format!("Nodes to resend query: {:?}", nodes_to_resend_query).as_str());
                let mut responses = vec![];
                let self_cloned = Arc::new(self.clone());
                for node_id in &nodes_to_resend_query {
                    responses.push(self_cloned.resend(&to_send, node_id));
                }

                let final_response =
                    match responses.iter().find(|&response| response.is_ok()).cloned() {
                        Some(response) => response,
                        None => Err("None of the responses were successful".to_string()),
                    };

                let _ = self
                    .logger
                    .log(format!("Create table response: {:?}", final_response).as_str());

                match final_response {
                    Ok(response) => Ok(QueryResult::SchemaChange {
                        change_type: response,
                        target: Default::default(),
                        options: Default::default(),
                    }),
                    Err(e) => {
                        eprintln!("{}", e);
                        Err(ErrorCode::Invalid)
                    }
                }
            }

            ParsedQuery::Insert {
                table_name,
                rows_to_insert,
                ..
            } => {
                let query_str = add_timestamp_to_insert_message(&query_str);

                let to_send = InternalMessage::Query {
                    opcode: 2,
                    body: query_str.to_string(),
                    keyspace_name: keyspace_name.clone(),
                };
                let mut nodes_to_resend_query =
                    self.get_nodes_for_insert(&keyspace_name, table_name, &rows_to_insert[0]);

                let _ = self
                    .logger
                    .log(format!("Nodes to resend query: {:?}", nodes_to_resend_query).as_str());

                let (tx, rx) = mpsc::sync_channel(1);
                let number_of_nodes_to_resend = nodes_to_resend_query.len();

                let nodes_to_check = nodes_to_resend_query.clone();

                if let Some(pos) = nodes_to_resend_query.iter().position(|x| *x == self.id) {
                    let response = self.receive_internal_message(&to_send);

                    match tx.send(response) {
                        Ok(_) => {
                            println!("Sent OK response to rx successfully");
                        }
                        Err(_) => {
                            println!("Consistency level already met");
                        }
                    };
                    nodes_to_resend_query.remove(pos);
                }

                for node_id in nodes_to_resend_query {
                    let self_cloned = Arc::new(self.clone());
                    let to_send = to_send.clone();
                    let tx = tx.clone();

                    std::thread::spawn(move || {
                        let response = self_cloned.resend(&to_send, &node_id);

                        match response {
                            Ok(response) => match tx.send(Ok(response)) {
                                Ok(_) => {
                                    println!("Sent OK response to rx successfully");
                                    drop(tx);
                                }
                                Err(_) => {
                                    println!("Consistency level already met");
                                    drop(tx);
                                }
                            },
                            Err(e) => match tx.send(Err(e)) {
                                Ok(_) => {
                                    println!("Sent Error response to rx successfully");
                                    drop(tx);
                                }
                                Err(_) => {
                                    println!("Consistency level already met");
                                    drop(tx);
                                }
                            },
                        }
                    });
                }
                drop(tx);

                match consistency_level.check_consistency_level(&rx, number_of_nodes_to_resend) {
                    Ok(_) => {
                        let _ = self.logger.log(
                            format!(
                                "Consistency level {:?} checked on: {:?}",
                                consistency_level,
                                nodes_to_check.to_vec()
                            )
                            .as_str(),
                        );

                        Ok(QueryResult::Void)
                    }
                    Err(_) => {
                        let _ = self.logger.log(
                            format!(
                                "Insert didn't meet consistency level on: {:?}",
                                nodes_to_check
                            )
                            .as_str(),
                        );

                        Err(ErrorCode::UnavailableException)
                    }
                }
            }
            ParsedQuery::Select {
                condition,
                //columns,
                table_name,
                ..
            } => {
                let to_send = InternalMessage::Query {
                    opcode: 3,
                    body: query_str.to_string(),
                    keyspace_name: keyspace_name.clone().to_string(),
                };
                let table_name_to_find = format!("{}.{}", keyspace_name, table_name);

                let data = match self.data.read() {
                    Ok(data) => data.clone(),

                    Err(_) => {
                        return Err(ErrorCode::UnavailableException);
                    }
                };

                if !data.contains_key(&table_name_to_find) {
                    // println!("Table not found");
                    return Err(ErrorCode::UnavailableException); // Table not found
                }

                let mut nodes_to_resend_query =
                    self.get_nodes_for_condition(&keyspace_name, condition);

                let _ = self
                    .logger
                    .log(format!("Nodes to resend query: {:?}", nodes_to_resend_query).as_str());

                let (tx, rx) = mpsc::channel();
                let number_of_nodes_to_resend = nodes_to_resend_query.len();

                let nodes_to_check = nodes_to_resend_query.clone();

                if let Some(pos) = nodes_to_resend_query.iter().position(|x| *x == self.id) {
                    let response = self.receive_internal_message(&to_send);
                    tx.send(response).unwrap();
                    nodes_to_resend_query.remove(pos);
                }

                for node_id in nodes_to_resend_query {
                    let to_send = to_send.clone();
                    let tx = tx.clone();
                    let cloned_gossip_table = local_gossip_table.clone();

                    std::thread::spawn(move || {
                        let response =
                            resend_without_storing_hint(&cloned_gossip_table, &to_send, &node_id);
                        match response {
                            Ok(response) => match tx.send(Ok(response)) {
                                Ok(_) => {
                                    println!("Sent OK response to rx successfully");
                                    drop(tx);
                                }
                                Err(_) => {
                                    println!("Consistency level already met");
                                    drop(tx);
                                }
                            },
                            Err(e) => match tx.send(Err(e)) {
                                Ok(_) => {
                                    println!("Sent Error response to rx successfully");
                                    drop(tx);
                                }
                                Err(_) => {
                                    println!("Consistency level already met");
                                    drop(tx);
                                }
                            },
                        }
                    });
                }

                match consistency_level.check_consistency_level(&rx, number_of_nodes_to_resend) {
                    Ok(responses) => {
                        // Vamos a comparar la columna timestamp de las responses para ver si hay que hacer read repair
                        // Si coinciden, devolvemos la respuesta 0
                        // Si no coinciden, vamos a hacer read repair
                        //     Vamos a ver cual es la respuesta con el timestamp mas grande
                        //     Luego enviamos insert a todos los nodos
                        let _ = self.logger.log(
                            format!(
                                "Consistency level {:?} checked on: {:?}",
                                consistency_level,
                                nodes_to_check.to_vec()
                            )
                            .as_str(),
                        );

                        let final_response =
                            self.read_repair(&responses, &keyspace_name, table_name);

                        Ok(QueryResult::parse_json_to_rows(&final_response))
                    }
                    Err(_) => {
                        let _ = self.logger.log(
                            format!(
                                "Select didn't meet consistency level on: {:?}",
                                nodes_to_check
                            )
                            .as_str(),
                        );
                        // println!("Error checking consistency level");
                        Err(ErrorCode::UnavailableException)
                    }
                }
            }

            ParsedQuery::Update { condition, .. } => {
                let query_str = add_timestamp_to_update_message(&query_str);

                let to_send = InternalMessage::Query {
                    opcode: 4,
                    body: query_str.to_string(),
                    keyspace_name: keyspace_name.clone(),
                };

                let mut nodes_to_resend_query =
                    self.get_nodes_for_condition(keyspace_name.as_str(), condition);

                let _ = self
                    .logger
                    .log(format!("Nodes to resend query: {:?}", nodes_to_resend_query).as_str());

                let (tx, rx) = mpsc::channel();
                let number_of_nodes_to_resend = nodes_to_resend_query.len();

                let nodes_to_check = nodes_to_resend_query.clone();
                if let Some(pos) = nodes_to_resend_query.iter().position(|x| *x == self.id) {
                    let response = self.receive_internal_message(&to_send);
                    if let Err(e) = tx.send(response) {
                        eprintln!("Error sending response to rx: {}", e);
                    }
                    nodes_to_resend_query.remove(pos);
                }

                for node_id in nodes_to_resend_query {
                    let self_cloned = Arc::new(self.clone());
                    let to_send = to_send.clone();
                    let tx = tx.clone();

                    std::thread::spawn(move || {
                        let response = self_cloned.resend(&to_send, &node_id);
                        match response {
                            Ok(response) => match tx.send(Ok(response)) {
                                Ok(_) => {
                                    println!("Sent OK response to rx successfully");
                                    drop(tx);
                                }
                                Err(_) => {
                                    println!("Consistency level already met");
                                    drop(tx);
                                }
                            },
                            Err(e) => match tx.send(Err(e)) {
                                Ok(_) => {
                                    println!("Sent Error response to rx successfully");
                                    drop(tx);
                                }
                                Err(_) => {
                                    println!("Consistency level already met");
                                    drop(tx);
                                }
                            },
                        }
                    });
                }

                match consistency_level.check_consistency_level(&rx, number_of_nodes_to_resend) {
                    Ok(_) => {
                        let _ = self.logger.log(
                            format!(
                                "Consistency level {:?} checked on: {:?}",
                                consistency_level,
                                nodes_to_check.to_vec()
                            )
                            .as_str(),
                        );
                        Ok(QueryResult::Void)
                    }

                    Err(_) => {
                        let _ = self.logger.log(
                            format!(
                                "Update didn't meet consistency level on: {:?}, returning Err",
                                nodes_to_check
                            )
                            .as_str(),
                        );
                        Err(ErrorCode::UnavailableException)
                    }
                }
            }
            ParsedQuery::Delete { condition, .. } => {
                let to_send = InternalMessage::Query {
                    opcode: 5,
                    body: query_str.to_string(),
                    keyspace_name: keyspace_name.clone(),
                };
                let mut nodes_to_resend_query =
                    self.get_nodes_for_condition(keyspace_name.as_str(), condition);

                let _ = self
                    .logger
                    .log(format!("Nodes to resend query: {:?}", nodes_to_resend_query).as_str());

                let (tx, rx) = mpsc::channel();
                let number_of_nodes_to_resend = nodes_to_resend_query.len();

                let nodes_to_check = nodes_to_resend_query.clone();
                if let Some(pos) = nodes_to_resend_query.iter().position(|x| *x == self.id) {
                    let response = self.receive_internal_message(&to_send);
                    tx.send(response).unwrap();
                    nodes_to_resend_query.remove(pos);
                }

                for node_id in nodes_to_resend_query {
                    let self_cloned = Arc::new(self.clone());
                    let to_send = to_send.clone();
                    let tx = tx.clone();
                    std::thread::spawn(move || {
                        let response = self_cloned.resend(&to_send, &node_id);
                        match response {
                            Ok(response) => match tx.send(Ok(response)) {
                                Ok(_) => {
                                    println!("Sent OK response to rx successfully");
                                    drop(tx);
                                }
                                Err(_) => {
                                    println!("Consistency level already met");
                                    drop(tx);
                                }
                            },
                            Err(e) => match tx.send(Err(e)) {
                                Ok(_) => {
                                    println!("Sent Error response to rx successfully");
                                    drop(tx);
                                }
                                Err(_) => {
                                    println!("Consistency level already met");
                                    drop(tx);
                                }
                            },
                        }
                    });
                }

                match consistency_level.check_consistency_level(&rx, number_of_nodes_to_resend) {
                    Ok(_) => {
                        let _ = self.logger.log(
                            format!(
                                "Consistency level {:?} checked on: {:?}",
                                consistency_level,
                                nodes_to_check.to_vec()
                            )
                            .as_str(),
                        );
                        Ok(QueryResult::Void)
                    }

                    Err(_) => {
                        let _ = self.logger.log(
                            format!(
                                "Delete didn't meet consistency level on: {:?}, returning Err",
                                nodes_to_check
                            )
                            .as_str(),
                        );
                        Err(ErrorCode::UnavailableException)
                    }
                }
            }
            ParsedQuery::UseKeyspace { keyspace_name } => {
                if self.keyspace_exists(keyspace_name) {
                    let _ = self
                        .logger
                        .log(format!("Keyspace changed to {}", keyspace_name).as_str());
                    Ok(QueryResult::SetKeyspace(keyspace_name.to_string()))
                } else {
                    let _ = self
                        .logger
                        .log(format!("Keyspace {} not found", keyspace_name).as_str());
                    Err(ErrorCode::Invalid)
                }
            }
        }
    }

    /// Resends an internal message (`to_send`) to a specified node identified by `node_id`.
    ///
    /// # Parameters
    /// - `to_send`: The internal message to be resent.
    /// - `node_id`: The id of the node to which the message will be sent.
    ///
    /// # Returns
    /// A `String` containing the response from the node, or an Err(String) if the operation failed.
    ///
    fn resend(&self, to_send: &InternalMessage, node_id: &str) -> Result<String, String> {
        if node_id == self.id {
            return self.receive_internal_message(to_send);
        }
        let mut port = "";
        let mut ip = "";

        let gossip_table = match self.get_gossip_table() {
            Ok(table) => table,
            Err(e) => {
                return Err(e);
            }
        };

        for gossip_info in &gossip_table {
            if gossip_info.node_id == node_id {
                ip = &gossip_info.ip;
                port = &gossip_info.port_gossip_query;
                break;
            }
        }

        if port.is_empty() {
            return Err("Node not found".to_string());
        }

        let destination = format!("{}:{}", ip, port);

        let _ = self
            .logger
            .log(format!("Attempting resend to {}", &destination).as_str());

        if let Ok(mut stream) = TcpStream::connect(&destination) {
            if let Err(e) = to_send.write_to_stream(&mut stream) {
                let _ = self
                    .logger
                    .log(format!("Error writing to stream while resending to node {}, storing query for hinted-handoff", &destination).as_str());
                let mut hints_for_all_nodes = match self.hints.write() {
                    Ok(hints) => hints,
                    Err(_) => {
                        return Err("Error locking hints".to_string());
                    }
                };
                if let Some(hints) = hints_for_all_nodes.get_mut(node_id) {
                    hints.push(to_send.clone());
                } else {
                    hints_for_all_nodes.insert(node_id.to_string(), vec![to_send.clone()]);
                }
                return Err(format!("Error resending query: {}", e));
            }
            let _ = self
                .logger
                .log(format!("Query resent to {}", &destination).as_str());

            let response = InternalMessage::deserialize_from_stream(&mut stream);

            if let Ok(response) = response {
                match response {
                    InternalMessage::Response { opcode, body } => {
                        if opcode == 0 {
                            Ok(body)
                        } else {
                            Err(body)
                        }
                    }
                    _ => Err("Invalid response".to_string()),
                }
            } else {
                Err("Error deserializing response".to_string())
            }
        } else {
            let _ = self.logger.log(
                format!(
                    "Error connecting to node {}, storing query for hinted-handoff",
                    &destination
                )
                .as_str(),
            );

            let mut hints_for_all_nodes = match self.hints.write() {
                Ok(hints) => hints,
                Err(_) => {
                    return Err("Error locking hints".to_string());
                }
            };
            if let Some(hints) = hints_for_all_nodes.get_mut(node_id) {
                hints.push(to_send.clone());
            } else {
                hints_for_all_nodes.insert(node_id.to_string(), vec![to_send.clone()]);
            }
            Err("Error connecting to node".to_string())
        }
    }

    // ------------------------ Receive and Execute Query ------------------------

    /// Handles the internal reception of messages (`message`) and processes them according to their type.
    ///
    /// # Parameters
    /// - `message`: The internal message to be processed.
    ///
    /// # Returns
    /// Ok(String) on success, or a descriptive Err(String) on failure.
    pub fn receive_internal_message(&self, message: &InternalMessage) -> Result<String, String> {
        match message {
            InternalMessage::Gossip { opcode, body } => {
                let gossip_table: Vec<GossipInformation> = match serde_json::from_str(body) {
                    Ok(table) => table,
                    Err(e) => return Err(format!("Error deserializing gossip table: {}", e)),
                };
                match opcode {
                    0 => {
                        // GOSSIP
                        // println!("[{}] Gossip received, updating gossip table", Utc::now().format("%Y-%m-%d %H:%M:%S"));
                        self.update_gossip_table(&gossip_table);
                        // println!("[{}] Gossip table updated successfully", Utc::now().format("%Y-%m-%d %H:%M:%S"));
                        Ok("Gossip received successfully".to_string())
                    }
                    1 => {
                        // NEW NODE
                        self.update_gossip_table(&gossip_table);
                        let local_gossip_table = match self.gossip_table.read() {
                            Ok(gossip_table) => gossip_table.clone(),
                            Err(_) => {
                                return Err("Error locking gossip table".to_string());
                            }
                        };
                        if let Ok(gossip_table_json) = gossip_table_to_json(&local_gossip_table) {
                            Ok(gossip_table_json)
                        } else {
                            Err("Error serializing gossip table".to_string())
                        }
                    }
                    _ => Err("Invalid opcode".to_string()),
                }
            }
            InternalMessage::Query {
                opcode,
                body,
                keyspace_name,
            } => {
                let _ = self
                    .logger
                    .log(format!("Received query internally: {}", body).as_str());

                let parsed_query = match parse_instruction(body) {
                    Ok(parsed_query) => parsed_query,
                    Err(e) => return Err(format!("Error parsing query: {}", e)),
                };

                match opcode {
                    0 => {
                        // CREATE KEYSPACE

                        match parsed_query {
                            ParsedQuery::CreateKeyspace {
                                keyspace_name,
                                replication_strategy,
                                replication_factor,
                            } => {
                                let result = self.create_keyspace(
                                    &keyspace_name,
                                    &replication_strategy,
                                    &replication_factor,
                                );
                                if let Err(e) = result {
                                    Err(e)
                                } else {
                                    let _ = self.logger.log(
                                        format!("Keyspace created: {}", keyspace_name).as_str(),
                                    );
                                    Ok("Keyspace created successfully".to_string())
                                }
                            }
                            _ => Err("Opcode doesn't match query".to_string()),
                        }
                    }
                    1 => {
                        // CREATE TABLE
                        match parsed_query {
                            ParsedQuery::CreateTable {
                                table_name,
                                partition_key_columns,
                                clustering_key_columns,
                                columns,
                            } => {
                                self.create_encrypted_table(
                                    keyspace_name,
                                    &table_name,
                                    partition_key_columns,
                                    clustering_key_columns,
                                    columns,
                                );
                                let _ = self
                                    .logger
                                    .log(format!("Table created: {}", table_name).as_str());
                                Ok("Table created successfully".to_string())
                            }
                            _ => Err("Opcode doesn't match query".to_string()),
                        }
                    }
                    2 => {
                        // INSERT
                        match parsed_query {
                            ParsedQuery::Insert {
                                table_name,
                                rows_to_insert,
                                ..
                            } => {
                                if let Some(row) = rows_to_insert.into_iter().next() {
                                    let result = self.insert_row(keyspace_name, &table_name, row);
                                    if let Err(e) = result {
                                        return Err(e);
                                    } else {
                                        let _ = self.logger.log(
                                            format!("Row inserted in table: {}", table_name)
                                                .as_str(),
                                        );
                                        return Ok("Row inserted successfully".to_string());
                                    }
                                }
                                Err("No rows to insert".to_string())
                            }
                            _ => Err("Opcode doesn't match query".to_string()),
                        }
                    }
                    3 => {
                        // SELECT
                        match parsed_query {
                            ParsedQuery::Select {
                                table_name,
                                columns: _,
                                condition,
                                order_by: _,
                            } => {
                                let table = match self.get_table(keyspace_name, &table_name) {
                                    Some(table) => table,
                                    None => return Err("Table not found".to_string()),
                                };

                                let rows = table.select_if(&condition);
                                let mut response = vec![];

                                for row in rows {
                                    response.push(row.clone());
                                }

                                match serde_json::to_string(&response) {
                                    Ok(json) => {
                                        let _ = self.logger.log(
                                            format!(
                                                "Returning select values from table: {}",
                                                table_name
                                            )
                                            .as_str(),
                                        );
                                        Ok(json)
                                    }
                                    Err(e) => Err(format!("Error serializing response: {}", e)),
                                }
                            }
                            _ => Err("Opcode doesn't match query".to_string()),
                        }
                    }
                    4 => {
                        // UPDATE
                        match parsed_query {
                            ParsedQuery::Update {
                                table_name,
                                values_to_update,
                                condition,
                            } => {
                                let result = self.update_row(
                                    keyspace_name,
                                    &table_name,
                                    values_to_update,
                                    &condition,
                                );
                                if let Err(e) = result {
                                    Err(e)
                                } else {
                                    let _ = self.logger.log(
                                        format!("Row updated in table: {}", table_name).as_str(),
                                    );
                                    Ok("Row updated successfully".to_string())
                                }
                            }
                            _ => Err("Opcode doesn't match query".to_string()),
                        }
                    }
                    5 => {
                        // DELETE
                        match parsed_query {
                            ParsedQuery::Delete {
                                table_name,
                                condition,
                            } => {
                                let result =
                                    self.delete_row(keyspace_name, &table_name, &condition);
                                if let Err(e) = result {
                                    Err(e)
                                } else {
                                    let _ = self.logger.log(
                                        format!("Row deleted in table: {}", table_name).as_str(),
                                    );
                                    Ok("Row deleted successfully".to_string())
                                }
                            }
                            _ => Err("Opcode doesn't match query".to_string()),
                        }
                    }
                    _ => Err("Invalid opcode".to_string()),
                }
            }
            InternalMessage::Response { .. } => {
                Err("Received response when should have received request".to_string())
            }
        }
    }

    // ------------------------  Getting nodes (Replication/Hash) ------------------------

    /// Retrieves a list of all live nodes based on the current gossip table.
    ///
    /// # Returns
    /// A `Vec<String>` containing the node ids of all live nodes or an empty vector if the operation failed.
    fn get_all_nodes(&self) -> Vec<String> {
        let mut nodes = vec![];

        let gossip_table = match self.gossip_table.read() {
            Ok(gossip_table) => gossip_table.clone(),

            Err(_) => {
                return Vec::new();
            }
        };

        for node in get_live_nodes(&gossip_table) {
            nodes.push(node.node_id.clone());
        }
        nodes
    }

    /// Retrieves the list of nodes for data insertion based on the provided keyspace, table, and row values.
    ///
    /// # Parameters
    /// - `keyspace_name`: The name of the keyspace.
    /// - `table_name`: The name of the table.
    /// - `row_values`: A `HashMap<String, String>` containing the values to be inserted.
    ///
    /// # Returns
    /// - `Vec<String>` containing the node ids of the nodes to which the data should be inserted or
    ///    an empty vector if the operation failed.
    fn get_nodes_for_insert(
        &self,
        keyspace_name: &str,
        table_name: &str,
        row_values: &HashMap<String, String>,
    ) -> Vec<String> {
        let data = match self.data.read() {
            Ok(data) => data.clone(),
            Err(_) => {
                return Vec::new();
            }
        };

        let partition_key_columns =
            match data.get(format!("{}.{}", keyspace_name, table_name).as_str()) {
                Some(table) => table.get_partition_key_columns(),
                None => {
                    eprintln!(
                        "No se encontró la tabla: keyspace_name: {}, table_name: {}",
                        keyspace_name, table_name
                    );
                    return Vec::new();
                }
            };
        let mut partition_keys = Vec::new();

        for key_column in partition_key_columns {
            if let Some(value) = row_values.get(&key_column) {
                partition_keys.push(value.to_string());
            } else {
                eprintln!("No se encontró la columna");
                return Vec::new();
            }
        }

        let keyspaces = match self.keyspaces.read() {
            Ok(keyspaces) => keyspaces.clone(),

            Err(_) => {
                return Vec::new();
            }
        };

        let gossip_table = match self.gossip_table.read() {
            Ok(gossip_table) => gossip_table.clone(),

            Err(_) => {
                return Vec::new();
            }
        };

        if let Some(replication_strategy) = keyspaces.get(keyspace_name) {
            replication_strategy.get_replica_nodes(
                &partition_keys,
                &gossip_table,
                &self.consistent_hash,
            )
        } else {
            eprintln!("No se encontró el keyspace: {}", keyspace_name);
            Vec::new()
        }
    }

    /// Retrieves the nodes responsible for the partition key based on a condition.
    ///
    /// # Parameters
    /// - `keyspace_name`: The name of the keyspace.
    /// - `condition`: The condition to be evaluated.
    ///
    /// # Returns
    /// - vector of node IDs that are responsible for the given partition key in the condition or
    ///   an empty vector if the operation failed.
    fn get_nodes_for_condition(&self, keyspace_name: &str, condition: &Expression) -> Vec<String> {
        // Se asume que la condicion es sobre la partition key, que a su vez es la unica key
        let partition_key = extract_value_supposing_column_equals_value(condition);

        let partition_keys = match partition_key {
            Some(key) => vec![key],
            None => {
                eprintln!("La suposicion condition: 'column = value' no se cumplio");
                return Vec::new();
            }
        };

        let keyspaces = match self.keyspaces.read() {
            Ok(keyspaces) => keyspaces.clone(),

            Err(_) => {
                return Vec::new();
            }
        };

        let gossip_table = match self.gossip_table.read() {
            Ok(gossip_table) => gossip_table.clone(),

            Err(_) => {
                return Vec::new();
            }
        };

        if let Some(replication_strategy) = keyspaces.get(keyspace_name) {
            replication_strategy.get_replica_nodes(
                &partition_keys,
                &gossip_table,
                &self.consistent_hash,
            )
        } else {
            eprintln!("No se encontró el keyspace: {}", keyspace_name);
            Vec::new()
        }
    }

    // ------------------------  Disk ------------------------

    /// Flushes the in-memory data and keyspace information to disk.
    ///
    pub fn flush(&self) {
        self.flush_keyspaces();
        self.flush_data();
    }

    fn flush_keyspaces(&self) {
        let keyspaces = match self.keyspaces.read() {
            Ok(keyspaces) => keyspaces.clone(),
            Err(_) => {
                return;
            }
        };

        let dir = format!("./data/{}", self.id);
        let file = format!("{}/keyspaces", dir);

        if let Err(e) = fs::create_dir_all(&dir) {
            eprintln!("Failed to create directory {}: {}", dir, e);
        }

        for (keyspace_name, replication_strategy) in keyspaces.iter() {
            let write = format!(
                "{},{},{}",
                keyspace_name,
                replication_strategy.get_name(),
                replication_strategy.get_replication_factor()
            );
            if let Err(e) = fs::write(&file, write) {
                eprintln!("Failed to write to file {}: {}", file, e);
            }
        }
    }

    fn flush_data(&self) {
        let data = match self.data.read() {
            Ok(data) => data.clone(),
            Err(_) => {
                return;
            }
        };

        for (_, encrypted_table) in data.iter() {
            let table = encrypted_table.get_table();

            let dir = format!("./data/{}", self.id);
            //let file = format!("{}/{}", dir, table_name);

            if let Err(e) = fs::create_dir_all(&dir) {
                eprintln!("Failed to create directory {}: {}", dir, e);
                continue;
            }

            // Escribe la tabla en el archivo.
            if let Err(e) = encrypted_table.write_to_disk(&dir, table.get_name()) {
                eprintln!("Failed to write to file {}: {}", dir, e);
            }
        }
    }

    /// Loads the in-memory data and keyspace information from disk.
    ///
    fn load_data(&self) {
        self.load_keyspaces();
        self.load_tables();
        self.load_gossip_table();
    }

    fn load_keyspaces(&self) {
        let keyspaces_data = match load_keyspaces(&self.id) {
            Ok(keyspaces_data) => keyspaces_data,
            Err(e) => {
                eprintln!("Error loading keyspaces: {}", e);
                return;
            }
        };

        let mut keyspaces = match self.keyspaces.write() {
            Ok(keyspaces) => keyspaces,
            Err(_) => {
                eprintln!("Error locking keyspaces");
                return;
            }
        };

        for keyspace_data in keyspaces_data {
            let keyspace_name = keyspace_data.0;
            let replication_strategy_name = keyspace_data.1;
            let replication_factor = keyspace_data.2;

            match replication_strategy_name.as_str() {
                "SimpleStrategy" => {
                    keyspaces.insert(
                        keyspace_name.to_string(),
                        ReplicationStrategy::new_simple(replication_factor.to_string()),
                    );
                }
                _ => {
                    eprintln!(
                        "Invalid replication strategy: {}",
                        replication_strategy_name
                    );
                    return;
                }
            }
        }
    }

    fn load_tables(&self) {
        let tables_path = match load_tables_path(&self.id) {
            Ok(tables_path) => tables_path,
            Err(e) => {
                eprintln!("Error loading table names: {}", e);
                return;
            }
        };

        let mut data = match self.data.write() {
            Ok(data) => data,
            Err(_) => {
                eprintln!("Error locking data");
                return;
            }
        };

        for table_path in tables_path {
            let encrypted_table = match EncryptedTable::load_table(&self.id, &table_path) {
                Ok(table) => table,
                Err(e) => {
                    eprintln!("Error loading table: {}", e);
                    return;
                }
            };
            let table = encrypted_table.get_table();
            let name = table.get_name().clone();
            data.insert(name, encrypted_table);
        }
    }

    fn load_gossip_table(&self) {
        let loaded_gossip_table = match load_gossip_table(&self.id) {
            Ok(gossip_table) => gossip_table,
            Err(e) => {
                eprintln!("Error loading gossip table: {}", e);
                return;
            }
        };

        if loaded_gossip_table.is_empty() {
            return;
        }

        let mut gossip_table = match self.gossip_table.write() {
            Ok(gossip_table) => gossip_table,
            Err(_) => {
                eprintln!("Error locking gossip table");
                return;
            }
        };
        // print for debugging
        for gossip_info in &loaded_gossip_table {
            println!("Node: {}, IP: {}, Port: {}", gossip_info.node_id, gossip_info.ip, gossip_info.port_gossip_query);
        }

        *gossip_table = loaded_gossip_table;
    }
}

// ------------------------  JSON / Format ------------------------

///  Serializes the given gossip table into a JSON string.
///
/// # Parameters
/// - `gossip_table`: A vector of `GossipInformation` containing the gossip table to be serialized.
///
/// # Returns
/// Ok(string) on success, or an Err(string) on failure.
///
pub fn gossip_table_to_json(gossip_table: &Vec<GossipInformation>) -> Result<String, String> {
    match serde_json::to_string(gossip_table) {
        Ok(json) => Ok(json),
        Err(e) => Err(format!("Error serializing gossip table: {}", e)),
    }
}

/// Generates a CQL `INSERT` statement for a given table and data.
///
/// # Parameters
/// - `table_name`: The name of the table.
/// - `data`: A `HashMap<String, String>` containing the data to be inserted.
///
/// # Returns
/// A `String` containing the generated CQL statement.
///

fn generate_insert_cql(table_name: &str, data: HashMap<String, String>) -> String {
    let columns: Vec<String> = data.keys().cloned().collect();
    let values: Vec<String> = data.values().cloned().collect();

    let columns_str = columns.join(", ");
    let values_str = values
        .iter()
        .map(|v| format!("'{}'", v))
        .collect::<Vec<String>>()
        .join(", ");

    format!(
        "INSERT INTO {} ({}) VALUES ({});",
        table_name, columns_str, values_str
    )
}

// ------------------------  Auxiliar ------------------------

/// Sends an internal message to a specified node in the gossip table and waits for a response.
/// 
/// # Parameters
/// - `message`: The `InternalMessage` to be sent.
/// - `ip`: The IP address of the node.
/// - `port`: The port of the node.
/// 
/// # Returns
/// Ok(InternalMessage::Response) if a response was received, or an Err(String).
///  
fn send_internal_message_and_return_response(
    message: &InternalMessage,
    ip: &str,
    port: &str,
) -> Result<InternalMessage, String> {
    let destination = format!("{}:{}", ip, port);
    match TcpStream::connect(destination) {
        Ok(mut stream) => {
            if let Err(e) = message.write_to_stream(&mut stream) {
                eprintln!("Error sending message: {}", e);
                return Err(format!("Error sending message: {}", e));
            }

            let response = InternalMessage::deserialize_from_stream(&mut stream);

            if let Ok(response) = response {
                match response {
                    InternalMessage::Response { .. } => {
                        Ok(response)
                    }
                    _ => {
                        eprintln!("Received invalid response");
                        Err("Invalid response".to_string())
                    },
                }
            } else {
                eprintln!("Error deserializing response");
                Err("Error deserializing response".to_string())
            }
        }
        Err(e) => {
            eprintln!("Error connecting to node: {}", e);
            Err(format!("Error connecting to node: {}", e))
        }
    }
}

/// Resends an `InternalMessage` to a specified node in the gossip table and waits for a response. Used for query types that don't require storing hints, as SELECT.
///
/// # Parameters
/// - `gossip_table`: A vector of `GossipInformation` containing the gossip table.
/// - `to_send`: The `InternalMessage` to be resent.
/// - `node_id`: The id of the node to which the message will be sent.
///
/// # Returns
/// Ok(String) on success, or a descriptive Err(String) on failure.
fn resend_without_storing_hint(
    gossip_table: &Vec<GossipInformation>,
    to_send: &InternalMessage,
    node_id: &str,
) -> Result<String, String> {
    let mut port = "";
    let mut ip = "";

    for gossip_info in gossip_table {
        if gossip_info.node_id == node_id {
            ip = &gossip_info.ip;
            port = &gossip_info.port_gossip_query;
            break;
        }
    }

    if port.is_empty() {
        return Err("Node not found".to_string());
    }

    let destination = format!("{}:{}", ip, port);

    if let Ok(mut stream) = TcpStream::connect(destination) {
        if let Err(e) = to_send.write_to_stream(&mut stream) {
            return Err(format!("Error resending query: {}", e));
        }

        let response = InternalMessage::deserialize_from_stream(&mut stream);

        if let Ok(response) = response {
            match response {
                InternalMessage::Response { opcode, body } => {
                    if opcode == 0 {
                        Ok(body)
                    } else {
                        Err(body)
                    }
                }
                _ => Err("Invalid response".to_string()),
            }
        } else {
            Err("Error deserializing response".to_string())
        }
    } else {
        Err("Error connecting to node".to_string())
    }
}

/// Retrieves all the live nodes from the given gossip table.
///
/// # Parameters
/// - `gossip_table`: A vector of `GossipInformation` containing the gossip table.
///
/// # Returns
/// A vector of `GossipInformation` containing the live nodes.
fn get_live_nodes(gossip_table: &Vec<GossipInformation>) -> Vec<GossipInformation> {
    let mut nodes = vec![];
    for node_info in gossip_table {
        if node_info.status == "Live" {
            nodes.push(node_info.clone());
        }
    }
    nodes
}

#[cfg(test)]
mod tests {
    use crate::query_parser::expression::Operand;

    use super::*;

    #[test]
    fn test_create_keyspace() {
        let node = Node::new("node1", "localhost", 9042, 7000);
        node.create_keyspace("test_keyspace", "SimpleStrategy", "3")
            .unwrap();
        assert!(node.keyspace_exists("test_keyspace"));
    }

    #[test]
    fn test_create_table() {
        let node = Node::new("node1", "localhost", 9042, 7000);
        let _ = node.create_keyspace("test_keyspace", "SimpleStrategy", "3");
        node.create_encrypted_table(
            "test_keyspace",
            "test_table",
            vec!["id".to_string()],
            vec!["name".to_string()],
            vec![
                ("id".to_string(), "int".to_string()),
                ("name".to_string(), "text".to_string()),
            ],
        );

        let data = match node.data.read() {
            Ok(data) => data.clone(),
            Err(_) => {
                return;
            }
        };

        assert!(data.contains_key("test_keyspace.test_table"));
    }

    #[test]
    fn test_insert_row() {
        let node = Node::new("node1", "localhost", 9042, 7000);
        let _ = node.create_keyspace("test_keyspace", "SimpleStrategy", "3");
        node.create_encrypted_table(
            "test_keyspace",
            "test_table",
            vec!["id".to_string()],
            vec!["name".to_string()],
            vec![
                ("id".to_string(), "int".to_string()),
                ("name".to_string(), "text".to_string()),
            ],
        );
        let mut values = HashMap::new();
        values.insert("id".to_string(), "1".to_string());
        values.insert("name".to_string(), "Alice".to_string());
        let result = node.insert_row("test_keyspace", "test_table", values);
        assert!(result.is_ok());
    }

    #[test]
    fn test_update_gossip_table() {
        let node = Node::new("node1", "localhost", 9042, 7000);
        let gossip_info = GossipInformation {
            node_id: "node2".to_string(),
            ip: "localhost".to_string(),
            port_native_protocol: "9042".to_string(),
            port_gossip_query: "7000".to_string(),
            last_heartbeat: 123456789,
            status: "UP".to_string(),
        };
        node.update_gossip_table(&vec![gossip_info.clone()]);

        let gossip_table = match node.gossip_table.read() {
            Ok(gossip_table) => gossip_table.clone(),
            Err(_) => {
                panic!("Error locking gossip table");
            }
        };

        assert_eq!(gossip_table.len(), 2);
        assert_eq!(gossip_table[1], gossip_info);
    }

    #[test]
    fn test_update_row() {
        let node = Node::new("node1", "localhost", 9042, 7000);
        let _ = node.create_keyspace("test_keyspace", "SimpleStrategy", "3");
        node.create_encrypted_table(
            "test_keyspace",
            "test_table",
            vec!["id".to_string()],
            vec!["name".to_string()],
            vec![
                ("id".to_string(), "int".to_string()),
                ("name".to_string(), "text".to_string()),
            ],
        );
        let mut values = HashMap::new();
        values.insert("id".to_string(), "1".to_string());
        values.insert("name".to_string(), "Alice".to_string());
        let _ = node.insert_row("test_keyspace", "test_table", values);

        let mut values_to_update = HashMap::new();
        values_to_update.insert("name".to_string(), "Bob".to_string());
        let condition = Expression::Comparison {
            left: Operand::Column("id".to_string()),
            operator: "=".to_string(),
            right: Operand::String("1".to_string()),
        };

        let mut expected_values = HashMap::new();

        expected_values.insert("id".to_string(), "1".to_string());
        expected_values.insert("name".to_string(), "Bob".to_string());

        let result = node.update_row(
            "test_keyspace",
            "test_table",
            values_to_update.clone(),
            &condition,
        );
        let data = match node.data.read() {
            Ok(data) => data.clone(),

            Err(_) => {
                return;
            }
        };

        assert!(result.is_ok());
        assert!(data
            .get("test_keyspace.test_table")
            .unwrap()
            .contains_row(&expected_values));
    }

    #[test]
    fn test_delete_row() {
        let node = Node::new("node1", "localhost", 9042, 7000);
        let _ = node.create_keyspace("test_keyspace", "SimpleStrategy", "3");
        node.create_encrypted_table(
            "test_keyspace",
            "test_table",
            vec!["id".to_string()],
            vec!["name".to_string()],
            vec![
                ("id".to_string(), "int".to_string()),
                ("name".to_string(), "text".to_string()),
            ],
        );
        let mut values = HashMap::new();
        values.insert("id".to_string(), "1".to_string());
        values.insert("name".to_string(), "Alice".to_string());
        let _ = node.insert_row("test_keyspace", "test_table", values.clone());

        let condition = Expression::Comparison {
            left: Operand::Column("id".to_string()),
            operator: "=".to_string(),
            right: Operand::String("1".to_string()),
        };

        let result = node.delete_row("test_keyspace", "test_table", &condition);

        let data = match node.data.read() {
            Ok(data) => data.clone(),

            Err(_) => {
                return;
            }
        };

        assert!(result.is_ok());
        assert!(!data
            .get("test_keyspace.test_table")
            .unwrap()
            .contains_row(&values));
    }
    #[test]
    fn test_insert_message_from_row_and_tablename() {
        let mut row = HashMap::new();
        row.insert("id".to_string(), "1".to_string());
        row.insert("name".to_string(), "Franco".to_string());
        row.insert("age".to_string(), "30".to_string());

        let table_name = "users";

        let expected = "INSERT INTO users (id, name, age) VALUES ('1', 'Franco', '30');";
        let result = insert_message_from_row_and_tablename(&row, table_name);

        assert_eq!(result, expected);
    }
}
