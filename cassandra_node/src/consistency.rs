use common::frame::messages::consistency_level::ConsistencyLevel;
use std::sync::mpsc::Receiver;

/// This enum has One, Quorum and All consistency levels.
/// 
/// 
#[derive(Debug)]
pub enum Consistency {
    One,
    Quorum,
    All,
}

impl Consistency {
    /*
    pub fn to_string(&self) -> String {
        match self {
            Consistency::One => "ONE".to_string(),
            Consistency::Quorum => "QUORUM".to_string(),
            Consistency::All => "ALL".to_string(),
        }
    }*/

    /// Converts native protocol's concistency level into consistency for nodes.
    /// 
    /// #Parameters
    /// - `consistency_level`: Level of consistency recieved from native protocol.
    /// 
    /// #Returns
    /// The same consistency (if it's not consistency level One, Quorum or All, returns One).
    pub fn from_consistency_level(consistency_level: ConsistencyLevel) -> Self {
        match consistency_level {
            ConsistencyLevel::One => Consistency::One,
            ConsistencyLevel::Quorum => Consistency::Quorum,
            ConsistencyLevel::All => Consistency::All,
            _ => Consistency::One,
        }
    }

    /// Returns the number of nodes to check to verify consistency level.
    /// 
    /// #Parameters
    /// - `nodes_to_resend_query`: number of nodes to which the query is sent.
    /// 
    /// #Returns
    /// Usize with the number of nodes to check.
    pub fn required_nodes(&self, nodes_to_resend_query: usize) -> usize {
        match self {
            Consistency::One => 1,
            Consistency::Quorum => nodes_to_resend_query / 2 + 1,
            Consistency::All => nodes_to_resend_query,
        }
    }
    /*
    pub fn from_str_to_enum(s: &str) -> Result<Self, String> {
        match s.to_uppercase().as_str() {
            "ONE" => Ok(Consistency::One),
            "QUORUM" => Ok(Consistency::Quorum),
            "ALL" => Ok(Consistency::All),
            _ => Err("Nivel de consistencia inválido".to_string()),
        }
    }*/

    /// Verifies if the consistency level is met.
    /// 
    /// #Parameters
    /// - `rx`: reciever that contains the respones from nodes.
    /// - `nodes_to_resend_query`: number of nodes to which the query is sent.
    /// 
    /// #Returns
    /// Ok(responses) if consistency is met or Err("No se alcanzó el consistency level") if it is not met.
    pub fn check_consistency_level(
        &self,
        rx: &Receiver<Result<String, String>>,
        nodes_to_resend_query: usize,
    ) -> Result<Vec<String>, String> {
        let mut total_recibidas = 0;
        let mut ok_recibidas = 0;
        let mut responses = vec![];
        while ok_recibidas < self.required_nodes(nodes_to_resend_query)
            && total_recibidas < nodes_to_resend_query
        {
            // agrego un timeout para que no se quede esperando infinitamente
            let response_received = rx.recv();

            match response_received {
                Ok(Ok(response)) => {
                    let mut string = String::new();
                    for c in response.chars() {
                        if c != '\0' {
                            string.push(c);
                        }
                    }
                    responses.push(string);
                    ok_recibidas += 1;
                    total_recibidas += 1;
                }
                Ok(Err(_)) => {
                    // println!("Error al recibir respuesta: {}", e);
                    total_recibidas += 1;
                }
                Err(_) => {
                    // println!("Se rompió la channel: {}", e);
                    break;
                }
            }
        }

        if responses.len() == self.required_nodes(nodes_to_resend_query) {
            //quito el print de las responses

            // println!("Se alcanzó el consistency level");

            Ok(responses.clone())
        } else {
            // println!("No se alcanzó el consistency level");
            Err("No se alcanzó el consistency level".to_string())
        }
    }
}
#[cfg(test)]
mod tests_consistency_lv {
    use std::io::Read;
    use std::net::TcpStream;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use common::client_manager::ClientManager;

    use crate::handler_nodes::{
        start_gossip, start_node_gossip_query_protocol, start_node_native_protocol,
    };
    use crate::internal_protocol::InternalMessage;
    use crate::node::{GossipInformation, Node};

    #[test]
    fn inserto_dato_con_consistency_level_one() -> Result<(), Box<dyn std::error::Error>> {
        let node1 = Arc::new(Node::new("Node1", "localhost", 51000, 61000));
        let node1_clone1 = Arc::clone(&node1);
        let node1_clone2 = Arc::clone(&node1);
        thread::spawn(move || start_node_native_protocol(node1_clone1));
        thread::spawn(move || start_node_gossip_query_protocol(node1_clone2));
        let node1_clone3 = Arc::clone(&node1);
        start_gossip(node1_clone3, 1000);

        let node2 = Arc::new(Node::new("Node2", "localhost", 51001, 61001));
        let node2_clone1 = Arc::clone(&node2);
        let node2_clone2 = Arc::clone(&node2);

        if let Ok(mut stream) = TcpStream::connect("localhost:61000") {
            let gossip_table = match node2.get_gossip_table() {
                Ok(gossip_table) => gossip_table,
                Err(e) => {
                    eprintln!("Error al obtener la gossip table: {}", e);
                    vec![]
                }
            };

            let gossip_messsage = InternalMessage::Gossip {
                opcode: 1,
                body: serde_json::to_string(&gossip_table).unwrap(),
            };

            if { gossip_messsage.write_to_stream(&mut stream) }.is_err() {
                println!("Error al enviar el mensaje de gossip:new node.");
            }

            let mut gossip_table_response = String::new();

            match stream.read_to_string(&mut gossip_table_response) {
                Ok(_) => {
                    println!(
                        "Gossip table recibido por nodo 2: {}",
                        gossip_table_response
                    );
                    let start = gossip_table_response.find('[').unwrap_or(0);
                    let gossip_table: Vec<GossipInformation> =
                        serde_json::from_str(&gossip_table_response[start..]).unwrap();
                    node2.update_gossip_table(&gossip_table);
                }
                Err(e) => {
                    println!("Error al leer del stream o timeout: {}", e);
                }
            }
        } else {
            println!("Error al conectar al nodo1.");
        }

        thread::spawn(move || start_node_native_protocol(node2_clone1));
        thread::spawn(move || start_node_gossip_query_protocol(node2_clone2));
        let node2_clone3 = Arc::clone(&node2);
        start_gossip(node2_clone3, 1000);
        thread::sleep(Duration::from_secs(3));

        let addresses: Vec<String> =
            vec!["localhost:s51000".to_string(), "localhost:51001".to_string()];
        let mut client_manager = ClientManager::new(&addresses)?;

        client_manager
            .query(
                "CREATE KEYSPACE flights_keyspace
                WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 2};"
                    .to_string(),
                "ONE",
            )
            .expect("Error al ejecutar la query");

        client_manager
            .query("USE flights_keyspace;".to_string(), "ONE")
            .expect("Error al ejecutar la query");

        client_manager
            .query(
                "CREATE TABLE flight_status_by_origin (
                    flight_id BIGINT,
                    origin_airport_id INT,
                    destination_airport_id INT,
                    departure_time TIMESTAMP,
                    arrival_time TIMESTAMP,
                    status TEXT,
                    airplane_id INT,
                    airplane_model TEXT,
                    max_fuel INT,
                    PRIMARY KEY ((origin_airport_id), departure_time)
                );"
                .to_string(),
                "ONE",
            )
            .expect("Error al ejecutar create table");

        client_manager.query("INSERT INTO flight_status_by_origin
                (flight_id, origin_airport_id, destination_airport_id, departure_time, arrival_time, status)
                VALUES (10001, 20, 8888, '2024-09-27 09:00:00', '2024-09-27 18:00:00', 'on_time');".to_string(),"ONE")
                .expect("Error al ejecutar insert");
        thread::sleep(Duration::from_secs(3));

        Ok(())
    }

    #[test]
    fn inserto_dato_con_consistency_level_quorum() -> Result<(), Box<dyn std::error::Error>> {
        let node1 = Arc::new(Node::new("Node1", "localhost", 52000, 62000));
        let node1_clone1 = Arc::clone(&node1);
        let node1_clone2 = Arc::clone(&node1);
        thread::spawn(move || start_node_native_protocol(node1_clone1));
        thread::spawn(move || start_node_gossip_query_protocol(node1_clone2));
        let node1_clone3 = Arc::clone(&node1);
        start_gossip(node1_clone3, 1000);

        let node2 = Arc::new(Node::new("Node2", "localhost", 52001, 62001));
        let node2_clone1 = Arc::clone(&node2);
        let node2_clone2 = Arc::clone(&node2);

        if let Ok(mut stream) = TcpStream::connect("localhost:62000") {
            let gossip_table = match node2.get_gossip_table() {
                Ok(gossip_table) => gossip_table,
                Err(e) => {
                    println!("Error al obtener la gossip table: {}", e);
                    vec![]
                }
            };

            let gossip_messsage = InternalMessage::Gossip {
                opcode: 1,
                body: serde_json::to_string(&gossip_table).unwrap(),
            };

            if { gossip_messsage.write_to_stream(&mut stream) }.is_err() {
                println!("Error al enviar el mensaje de gossip:new node.");
            }

            let mut gossip_table_response = String::new();

            match stream.read_to_string(&mut gossip_table_response) {
                Ok(_) => {
                    println!(
                        "Gossip table recibido por nodo 2: {}",
                        gossip_table_response
                    );
                    let start = gossip_table_response.find('[').unwrap_or(0);
                    let gossip_table: Vec<GossipInformation> =
                        serde_json::from_str(&gossip_table_response[start..]).unwrap();
                    node2.update_gossip_table(&gossip_table);
                }
                Err(e) => {
                    println!("Error al leer del stream o timeout: {}", e);
                }
            }
        } else {
            println!("Error al conectar al nodo1.");
        }

        thread::spawn(move || start_node_native_protocol(node2_clone1));
        thread::spawn(move || start_node_gossip_query_protocol(node2_clone2));
        let node2_clone3 = Arc::clone(&node2);
        start_gossip(node2_clone3, 1000);

        let node3 = Arc::new(Node::new("Node3", "localhost", 52002, 62002));
        let node3_clone1 = Arc::clone(&node3);
        let node3_clone2 = Arc::clone(&node3);

        if let Ok(mut stream) = TcpStream::connect("localhost:62000") {
            let gossip_table = match node3.get_gossip_table() {
                Ok(gossip_table) => gossip_table,
                Err(e) => {
                    println!("Error al obtener la gossip table: {}", e);
                    vec![]
                }
            };

            let gossip_messsage = InternalMessage::Gossip {
                opcode: 1,
                body: serde_json::to_string(&gossip_table).unwrap(),
            };

            if { gossip_messsage.write_to_stream(&mut stream) }.is_err() {
                println!("Error al enviar el mensaje de gossip:new node.");
            }

            let mut gossip_table_response = String::new();

            match stream.read_to_string(&mut gossip_table_response) {
                Ok(_) => {
                    println!(
                        "Gossip table recibido por nodo 3: {}",
                        gossip_table_response
                    );
                    let start = gossip_table_response.find('[').unwrap_or(0);
                    let gossip_table: Vec<GossipInformation> =
                        serde_json::from_str(&gossip_table_response[start..]).unwrap();
                    node3.update_gossip_table(&gossip_table);
                }
                Err(e) => {
                    println!("Error al leer del stream o timeout: {}", e);
                }
            }
        } else {
            println!("Error al conectar al nodo1.");
        }

        thread::spawn(move || start_node_native_protocol(node3_clone1));
        thread::spawn(move || start_node_gossip_query_protocol(node3_clone2));
        let node3_clone3 = Arc::clone(&node3);
        start_gossip(node3_clone3, 1000);
        thread::sleep(Duration::from_secs(3));

        let addresses: Vec<String> = vec![
            "localhost:52000".to_string(),
            "localhost:52001".to_string(),
            "localhost:52002".to_string(),
        ];
        let mut client_manager = ClientManager::new(&addresses)?;

        client_manager
            .query(
                "CREATE KEYSPACE flights_keyspace
                WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 3};"
                    .to_string(),
                "ONE",
            )
            .expect("Error al ejecutar la query");

        client_manager
            .query("USE flights_keyspace;".to_string(), "ONE")
            .expect("Error al ejecutar la query");

        client_manager
            .query(
                "CREATE TABLE flight_status_by_origin (
                    flight_id BIGINT,
                    origin_airport_id INT,
                    destination_airport_id INT,
                    departure_time TIMESTAMP,
                    arrival_time TIMESTAMP,
                    status TEXT,
                    airplane_id INT,
                    airplane_model TEXT,
                    max_fuel INT,
                    PRIMARY KEY ((origin_airport_id), departure_time)
                );"
                .to_string(),
                "ONE",
            )
            .expect("Error al ejecutar create table");

        client_manager.query("INSERT INTO flight_status_by_origin
                (flight_id, origin_airport_id, destination_airport_id, departure_time, arrival_time, status)
                VALUES (10001, 20, 8888, '2024-09-27 09:00:00', '2024-09-27 18:00:00', 'on_time');".to_string(),"QUORUM")
                .expect("Error al ejecutar insert");

        thread::sleep(Duration::from_secs(3));

        Ok(())
    }

    #[test]
    fn inserto_dato_con_consistency_level_all() -> Result<(), Box<dyn std::error::Error>> {
        let node1 = Arc::new(Node::new("Node1", "localhost", 53000, 63000));
        let node1_clone1 = Arc::clone(&node1);
        let node1_clone2 = Arc::clone(&node1);
        thread::spawn(move || start_node_native_protocol(node1_clone1));
        thread::spawn(move || start_node_gossip_query_protocol(node1_clone2));
        let node1_clone3 = Arc::clone(&node1);
        start_gossip(node1_clone3, 1000);

        let node2 = Arc::new(Node::new("Node2", "localhost", 53001, 63001));
        let node2_clone1 = Arc::clone(&node2);
        let node2_clone2 = Arc::clone(&node2);

        if let Ok(mut stream) = TcpStream::connect("localhost:63000") {
            let gossip_table = match node2.get_gossip_table() {
                Ok(gossip_table) => gossip_table,
                Err(e) => {
                    println!("Error al obtener la gossip table: {}", e);
                    vec![]
                }
            };

            let gossip_messsage = InternalMessage::Gossip {
                opcode: 1,
                body: serde_json::to_string(&gossip_table).unwrap(),
            };

            if { gossip_messsage.write_to_stream(&mut stream) }.is_err() {
                println!("Error al enviar el mensaje de gossip:new node.");
            }

            let mut gossip_table_response = String::new();

            match stream.read_to_string(&mut gossip_table_response) {
                Ok(_) => {
                    println!(
                        "Gossip table recibido por nodo 2: {}",
                        gossip_table_response
                    );
                    let start = gossip_table_response.find('[').unwrap_or(0);
                    let gossip_table: Vec<GossipInformation> =
                        serde_json::from_str(&gossip_table_response[start..]).unwrap();
                    node2.update_gossip_table(&gossip_table);
                }
                Err(e) => {
                    println!("Error al leer del stream o timeout: {}", e);
                }
            }
        } else {
            println!("Error al conectar al nodo1.");
        }

        thread::spawn(move || start_node_native_protocol(node2_clone1));
        thread::spawn(move || start_node_gossip_query_protocol(node2_clone2));
        let node2_clone3 = Arc::clone(&node2);
        start_gossip(node2_clone3, 1000);

        let node3 = Arc::new(Node::new("Node3", "localhost", 53002, 63002));
        let node3_clone1 = Arc::clone(&node3);
        let node3_clone2 = Arc::clone(&node3);

        if let Ok(mut stream) = TcpStream::connect("localhost:63000") {
            let gossip_table = match node3.get_gossip_table() {
                Ok(gossip_table) => gossip_table,
                Err(e) => {
                    println!("Error al obtener la gossip table: {}", e);
                    vec![]
                }
            };

            let gossip_messsage = InternalMessage::Gossip {
                opcode: 1,
                body: serde_json::to_string(&gossip_table).unwrap(),
            };

            if { gossip_messsage.write_to_stream(&mut stream) }.is_err() {
                println!("Error al enviar el mensaje de gossip:new node.");
            }

            let mut gossip_table_response = String::new();

            match stream.read_to_string(&mut gossip_table_response) {
                Ok(_) => {
                    println!(
                        "Gossip table recibido por nodo 3: {}",
                        gossip_table_response
                    );
                    let start = gossip_table_response.find('[').unwrap_or(0);
                    let gossip_table: Vec<GossipInformation> =
                        serde_json::from_str(&gossip_table_response[start..]).unwrap();
                    node3.update_gossip_table(&gossip_table);
                }
                Err(e) => {
                    println!("Error al leer del stream o timeout: {}", e);
                }
            }
        } else {
            println!("Error al conectar al nodo1.");
        }

        thread::spawn(move || start_node_native_protocol(node3_clone1));
        thread::spawn(move || start_node_gossip_query_protocol(node3_clone2));
        let node3_clone3 = Arc::clone(&node3);
        start_gossip(node3_clone3, 1000);
        thread::sleep(Duration::from_secs(3));

        let addresses: Vec<String> = vec![
            "localhost:53000".to_string(),
            "localhost:53001".to_string(),
            "localhost:53002".to_string(),
        ];
        let mut client_manager = ClientManager::new(&addresses)?;

        client_manager
            .query(
                "CREATE KEYSPACE flights_keyspace
                WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 3};"
                    .to_string(),
                "ONE",
            )
            .expect("Error al ejecutar la query");

        client_manager
            .query("USE flights_keyspace;".to_string(), "ONE")
            .expect("Error al ejecutar la query");

        client_manager
            .query(
                "CREATE TABLE flight_status_by_origin (
                    flight_id BIGINT,
                    origin_airport_id INT,
                    destination_airport_id INT,
                    departure_time TIMESTAMP,
                    arrival_time TIMESTAMP,
                    status TEXT,
                    airplane_id INT,
                    airplane_model TEXT,
                    max_fuel INT,
                    PRIMARY KEY ((origin_airport_id), departure_time)
                );"
                .to_string(),
                "ONE",
            )
            .expect("Error al ejecutar create table");

        client_manager.query("INSERT INTO flight_status_by_origin
                (flight_id, origin_airport_id, destination_airport_id, departure_time, arrival_time, status)
                VALUES (10001, 20, 8888, '2024-09-27 09:00:00', '2024-09-27 18:00:00', 'on_time');".to_string(),"ALL")
                .expect("Error al ejecutar insert");

        thread::sleep(Duration::from_secs(3));

        Ok(())
    }
}
