use std::cmp::min;

use crate::consistent_hashing::ConsistentHash;
use crate::node::GossipInformation;
use rand::{rng, Rng};

/// This enum has simple and random replication strategies.
/// 
/// 
#[derive(Clone, Debug)]
pub enum ReplicationStrategy {
    /// SimpleStrategy repilca la query al nodo correspondiente a la partición y a los siguientes nodos en el hash.
    SimpleStrategy { replication_factor: usize },
    /// RandomStrategy replica la query al nodo correspondiente a la partición y a nodos aleatorios en el hash.
    RandomStrategy { replication_factor: usize },
}

fn replication_factor_string_to_usize(replication_factor: String) -> usize {
    match replication_factor.as_str() {
        "1" | "ONE" => 1,
        "2" | "TWO" => 2,
        "3" | "THREE" => 3,
        "4" | "FOUR" => 4,
        "5" | "FIVE" => 5,
        "6" | "SIX" => 6,
        "7" | "SEVEN" => 7,
        "8" | "EIGHT" => 8,
        &_ => 1,
    }
}

impl ReplicationStrategy {
    /// Crea una nueva instancia de ReplicationStrategy con SimpleStrategy.
    pub fn new_simple(replication_factor: String) -> Self {
        ReplicationStrategy::SimpleStrategy {
            replication_factor: replication_factor_string_to_usize(replication_factor),
        }
    }

    /// Crea una nueva instancia de ReplicationStrategy con RandomStrategy.
    pub fn new_random(replication_factor: String) -> Self {
        ReplicationStrategy::RandomStrategy {
            replication_factor: replication_factor_string_to_usize(replication_factor),
        }
    }

    /// Get strategy name.
    /// 
    /// #Returns
    /// Strategy name.
    pub fn get_name(&self) -> String {
        match self {
            Self::SimpleStrategy { .. } => "SimpleStrategy".to_string(),
            Self::RandomStrategy { .. } => "RandomStrategy".to_string(),
        }
    }

    /// Get Replication factor.
    /// 
    /// #Returns
    /// Usize of replication factor.
    pub fn get_replication_factor(&self) -> usize {
        match self {
            Self::SimpleStrategy { replication_factor } => *replication_factor,
            Self::RandomStrategy { replication_factor } => *replication_factor,
        }
    }

    /// Gets the nodes to which the query is sent given the partition keys, gossip table, and hash.
    /// 
    /// #Parameters
    /// - `partition_keys`: Vector of partition keys
    /// - `gossip_table`: Contains gossip information of nodes.
    /// - `hash`: Consistent hashing.
    /// 
    /// #Returns
    /// A vector of node ids.
    pub fn get_replica_nodes(
        &self,
        partition_keys: &Vec<String>,
        gossip_table: &[GossipInformation],
        hash: &ConsistentHash,
    ) -> Vec<String> {
        match self {
            Self::SimpleStrategy { replication_factor } => {
                let mut nodes_to_send_query = vec![];
                let number_of_replicas = min(*replication_factor, gossip_table.len());
                for i in 0..number_of_replicas {
                    if let Ok(node_id) = hash.get_node_id(partition_keys, gossip_table, i) {
                        nodes_to_send_query.push(node_id);
                    } else {
                        return vec![];
                    }
                }

                nodes_to_send_query
            }
            Self::RandomStrategy { replication_factor } => {
                let mut nodes_to_send_query = vec![];
                if let Ok(node_id) = hash.get_node_id(partition_keys, gossip_table, 0) {
                    nodes_to_send_query.push(node_id);
                } else {
                    return vec![];
                }
                let number_of_replicas = min(*replication_factor, gossip_table.len());
                let mut rng = rng();
                let mut random_offsets = vec![];
                while random_offsets.len() < number_of_replicas {
                    let num = rng.random_range(1..gossip_table.len() - 1);
                    if !random_offsets.contains(&num) {
                        random_offsets.push(num);
                    }
                }

                for offset in random_offsets {
                    if let Ok(node_id) = hash.get_node_id(partition_keys, gossip_table, offset) {
                        nodes_to_send_query.push(node_id);
                    } else {
                        return vec![];
                    }
                }

                nodes_to_send_query
            }
        }
    }

   
}

#[cfg(test)]
mod tests_rf {
    use std::collections::HashMap;
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::sync::Arc;
    use std::thread;

    use crate::handler_nodes::{
        start_gossip, start_node_gossip_query_protocol, start_node_native_protocol_without_native,
    };
    use crate::internal_protocol::InternalMessage;

    use crate::node::{GossipInformation, Node};

    #[test]
    // testeo el aramado de un simple strategy con rf = 1
    fn creo_2_nodos_con_un_rf_de_1_y_envio_una_query() {
        let nodo1 = Arc::new(Node::new("Nodo1", "localhost", 1198, 1199));
        let nodo2 = Arc::new(Node::new("Nodo2", "localhost", 2298, 2299));

        let node1_clone = Arc::clone(&nodo1);

        thread::spawn(move || start_node_gossip_query_protocol(node1_clone));

        let node2_clone = Arc::clone(&nodo2);
        let node2_clone_native = Arc::clone(&nodo2);
        thread::spawn(move || start_node_native_protocol_without_native(node2_clone_native));
        thread::spawn(move || start_node_gossip_query_protocol(node2_clone));

        thread::sleep(std::time::Duration::from_millis(1000));

        for node in vec![&nodo2].into_iter() {
            if let Ok(mut stream) = TcpStream::connect("localhost:1199") {
                let gossip_table = node.get_gossip_table().unwrap_or_default();

                let gossip_messsage = InternalMessage::Gossip {
                    opcode: 1,
                    body: serde_json::to_string(&gossip_table).unwrap(),
                };

                if { gossip_messsage.write_to_stream(&mut stream) }.is_err() {
                    eprintln!("Error al enviar el mensaje de gossip:new node.");
                }

                let mut gossip_table_response = String::new();
                match stream.read_to_string(&mut gossip_table_response) {
                    Ok(_) => {
                        let start = gossip_table_response.find('[').unwrap_or(0);
                        let gossip_table: Vec<GossipInformation> =
                            serde_json::from_str(&gossip_table_response[start..]).unwrap();
                        node.update_gossip_table(&gossip_table);
                    }
                    Err(e) => {
                        eprintln!("Error al leer del stream o timeout: {}", e);
                    }
                }
            } else {
                eprintln!("Error al conectar al nodo1.");
            }
        }
        thread::sleep(std::time::Duration::from_millis(1000));

        for node in [Arc::clone(&nodo1), Arc::clone(&nodo2)] {
            start_gossip(node, 1000);
        }

        println!("Espero 3 segundos");
        thread::sleep(std::time::Duration::from_secs(2));

        let mut queries = vec![];
        queries.push("CREATE KEYSPACE keyspace1 WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1};");
        queries.push("USE keyspace1;");
        queries.push("CREATE TABLE vuelos (id_flight INT, from_city TEXT, to_city TEXT, departure_time TEXT, PRIMARY KEY ((from_city), departure_time));");
        queries.push("INSERT INTO vuelos (id_flight, from_city, to_city, departure_time) VALUES (1010, 'Rio', 'Catamarca', '21');");

        for query in queries {
            if let Ok(mut stream) = TcpStream::connect("localhost:2298") {
                stream.write_all(query.as_bytes()).unwrap();
                println!("Envio la query");
            } else {
                println!("Error al conectar al nodo1.");
            }
            thread::sleep(std::time::Duration::from_millis(1000));
        }

        thread::sleep(std::time::Duration::from_millis(2000));

        let mut values_vuelos1 = HashMap::new();
        values_vuelos1.insert("id_flight".to_string(), "1010".to_string());
        values_vuelos1.insert("from_city".to_string(), "Rio".to_string());
        values_vuelos1.insert("to_city".to_string(), "Catamarca".to_string());
        values_vuelos1.insert("departure_time".to_string(), "21".to_string());

        assert!(nodo1
            .get_table("keyspace1", "vuelos")
            .unwrap()
            .get_vector_of_rows()
            .contains(&values_vuelos1));

        //Como el rf = 1, el nodo 2 no contiene una copia de los datos
        assert!(!nodo2
            .get_table("keyspace1", "vuelos")
            .unwrap()
            .get_vector_of_rows()
            .contains(&values_vuelos1));
    }

    #[test]
    // testeo el aramado de un simple strategy con rf = 2
    fn creo_2_nodos_con_un_rf_de_2_y_envio_varios_inserts() {
        let nodo1 = Arc::new(Node::new("Nodo1", "localhost", 8001, 8002));
        let nodo2 = Arc::new(Node::new("Nodo2", "localhost", 9001, 9002));

        let node1_clone = Arc::clone(&nodo1);

        thread::spawn(move || start_node_gossip_query_protocol(node1_clone));

        let node2_clone = Arc::clone(&nodo2);
        let node2_clone_native = Arc::clone(&nodo2);
        thread::spawn(move || start_node_native_protocol_without_native(node2_clone_native));
        thread::spawn(move || start_node_gossip_query_protocol(node2_clone));

        thread::sleep(std::time::Duration::from_millis(1000));

        for node in vec![&nodo2].into_iter() {
            if let Ok(mut stream) = TcpStream::connect("localhost:8002") {
                let gossip_table = node.get_gossip_table().unwrap_or_default();

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
                        let start = gossip_table_response.find('[').unwrap_or(0);
                        let gossip_table: Vec<GossipInformation> =
                            serde_json::from_str(&gossip_table_response[start..]).unwrap();
                        node.update_gossip_table(&gossip_table);
                    }
                    Err(e) => {
                        println!("Error al leer del stream o timeout: {}", e);
                    }
                }
            } else {
                println!("Error al conectar al nodo1.");
            }
        }
        thread::sleep(std::time::Duration::from_millis(1000));

        for node in [Arc::clone(&nodo1), Arc::clone(&nodo2)] {
            start_gossip(node, 1000);
        }

        println!("Espero 3 segundos");
        thread::sleep(std::time::Duration::from_secs(2));

        let mut queries = vec![];
        queries.push("CREATE KEYSPACE keyspace1 WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2};");
        queries.push("USE keyspace1;");
        queries.push("CREATE TABLE vuelos (id_flight INT, from_city TEXT, to_city TEXT, departure_time TEXT, PRIMARY KEY ((from_city), departure_time));");
        queries.push("INSERT INTO vuelos (id_flight, from_city, to_city, departure_time) VALUES (1010, 'Rio', 'Catamarca', '21');");
        queries.push("INSERT INTO vuelos (id_flight, from_city, to_city, departure_time) VALUES (1011, 'Bariloche', 'Chubut', '22');");

        for query in queries {
            if let Ok(mut stream) = TcpStream::connect("localhost:9001") {
                stream.write_all(query.as_bytes()).unwrap();
                println!("Envio la query");
            } else {
                println!("Error al conectar al nodo1.");
            }
            thread::sleep(std::time::Duration::from_millis(1000));
        }

        thread::sleep(std::time::Duration::from_millis(2000));

        let mut values_vuelos1 = HashMap::new();
        values_vuelos1.insert("id_flight".to_string(), "1010".to_string());
        values_vuelos1.insert("from_city".to_string(), "Rio".to_string());
        values_vuelos1.insert("to_city".to_string(), "Catamarca".to_string());
        values_vuelos1.insert("departure_time".to_string(), "21".to_string());

        let mut values_vuelos2 = HashMap::new();
        values_vuelos2.insert("id_flight".to_string(), "1011".to_string());
        values_vuelos2.insert("from_city".to_string(), "Bariloche".to_string());
        values_vuelos2.insert("to_city".to_string(), "Chubut".to_string());
        values_vuelos2.insert("departure_time".to_string(), "22".to_string());

        assert!(nodo1
            .get_table("keyspace1", "vuelos")
            .unwrap()
            .get_vector_of_rows()
            .contains(&values_vuelos1));

        assert!(nodo1
            .get_table("keyspace1", "vuelos")
            .unwrap()
            .get_vector_of_rows()
            .contains(&values_vuelos2));

        //Como el rf = 2, el nodo 2 contiene una copia de los datos
        assert!(nodo2
            .get_table("keyspace1", "vuelos")
            .unwrap()
            .get_vector_of_rows()
            .contains(&values_vuelos1));

        assert!(nodo2
            .get_table("keyspace1", "vuelos")
            .unwrap()
            .get_vector_of_rows()
            .contains(&values_vuelos2));
    }

    #[test]
    // testeo el aramado de un random strategy con rf = 5

    fn creo_5_nodos_con_un_rf_de_5_y_envio_varios_inserts() {
        let nodo1 = Arc::new(Node::new("Nodo1", "localhost", 11112, 11113));
        let nodo2 = Arc::new(Node::new("Nodo2", "localhost", 22223, 22224));
        let nodo3 = Arc::new(Node::new("Nodo3", "localhost", 33334, 33335));
        let nodo4 = Arc::new(Node::new("Nodo4", "localhost", 44445, 44449));
        let nodo5 = Arc::new(Node::new("Nodo5", "localhost", 55556, 55557));

        let node1_clone = Arc::clone(&nodo1);

        thread::spawn(move || start_node_gossip_query_protocol(node1_clone));

        let node2_clone = Arc::clone(&nodo2);
        let node2_clone_native: Arc<Node> = Arc::clone(&nodo2);
        thread::spawn(move || start_node_native_protocol_without_native(node2_clone_native));
        thread::spawn(move || start_node_gossip_query_protocol(node2_clone));

        let node3_clone = Arc::clone(&nodo3);
        thread::spawn(move || start_node_gossip_query_protocol(node3_clone));

        let node4_clone = Arc::clone(&nodo4);
        thread::spawn(move || start_node_gossip_query_protocol(node4_clone));

        let node5_clone = Arc::clone(&nodo5);
        thread::spawn(move || start_node_gossip_query_protocol(node5_clone));

        thread::sleep(std::time::Duration::from_millis(1000));

        for node in vec![&nodo2, &nodo3, &nodo4, &nodo5].into_iter() {
            if let Ok(mut stream) = TcpStream::connect("localhost:11113") {
                let gossip_table = node.get_gossip_table().unwrap_or_default();

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
                        let start = gossip_table_response.find('[').unwrap_or(0);
                        let gossip_table: Vec<GossipInformation> =
                            serde_json::from_str(&gossip_table_response[start..]).unwrap();
                        node.update_gossip_table(&gossip_table);
                    }
                    Err(e) => {
                        println!("Error al leer del stream o timeout: {}", e);
                    }
                }
            } else {
                println!("Error al conectar al nodo1.");
            }
        }
        thread::sleep(std::time::Duration::from_millis(1000));

        for node in [
            Arc::clone(&nodo1),
            Arc::clone(&nodo2),
            Arc::clone(&nodo3),
            Arc::clone(&nodo4),
            Arc::clone(&nodo5),
        ] {
            start_gossip(node, 1000);
        }

        println!("Espero 3 segundos");
        thread::sleep(std::time::Duration::from_secs(5));

        let mut queries = vec![];
        queries.push("CREATE KEYSPACE keyspace1 WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 5 };");
        queries.push("USE keyspace1;");
        queries.push("CREATE TABLE vuelos (id_flight INT, from_city TEXT, to_city TEXT, departure_time TEXT, PRIMARY KEY ((from_city), departure_time));");
        queries.push("INSERT INTO vuelos (id_flight, from_city, to_city, departure_time) VALUES (1010, 'Rio', 'Catamarca', '21');");
        queries.push("INSERT INTO vuelos (id_flight, from_city, to_city, departure_time) VALUES (1011, 'Bariloche', 'Chubut', '22');");

        for query in queries {
            if let Ok(mut stream) = TcpStream::connect("localhost:22223") {
                stream.write_all(query.as_bytes()).unwrap();
                println!("Envio la query");
            } else {
                println!("Error al conectar al nodo2.");
            }
            thread::sleep(std::time::Duration::from_millis(1000));
        }

        thread::sleep(std::time::Duration::from_millis(5000));

        let mut values_vuelos1 = HashMap::new();
        values_vuelos1.insert("id_flight".to_string(), "1010".to_string());
        values_vuelos1.insert("from_city".to_string(), "Rio".to_string());
        values_vuelos1.insert("to_city".to_string(), "Catamarca".to_string());
        values_vuelos1.insert("departure_time".to_string(), "21".to_string());

        let mut values_vuelos2 = HashMap::new();
        values_vuelos2.insert("id_flight".to_string(), "1011".to_string());
        values_vuelos2.insert("from_city".to_string(), "Bariloche".to_string());
        values_vuelos2.insert("to_city".to_string(), "Chubut".to_string());
        values_vuelos2.insert("departure_time".to_string(), "22".to_string());

        assert!(nodo1
            .get_table("keyspace1", "vuelos")
            .unwrap()
            .get_vector_of_rows()
            .contains(&values_vuelos1));

        assert!(nodo1
            .get_table("keyspace1", "vuelos")
            .unwrap()
            .get_vector_of_rows()
            .contains(&values_vuelos2));

        assert!(nodo2
            .get_table("keyspace1", "vuelos")
            .unwrap()
            .get_vector_of_rows()
            .contains(&values_vuelos1));

        assert!(nodo2
            .get_table("keyspace1", "vuelos")
            .unwrap()
            .get_vector_of_rows()
            .contains(&values_vuelos2));

        assert!(nodo3
            .get_table("keyspace1", "vuelos")
            .unwrap()
            .get_vector_of_rows()
            .contains(&values_vuelos1));

        assert!(nodo3
            .get_table("keyspace1", "vuelos")
            .unwrap()
            .get_vector_of_rows()
            .contains(&values_vuelos2));

        assert!(nodo4
            .get_table("keyspace1", "vuelos")
            .unwrap()
            .get_vector_of_rows()
            .contains(&values_vuelos1));

        assert!(nodo4
            .get_table("keyspace1", "vuelos")
            .unwrap()
            .get_vector_of_rows()
            .contains(&values_vuelos2));

        assert!(nodo5
            .get_table("keyspace1", "vuelos")
            .unwrap()
            .get_vector_of_rows()
            .contains(&values_vuelos1));

        assert!(nodo5
            .get_table("keyspace1", "vuelos")
            .unwrap()
            .get_vector_of_rows()
            .contains(&values_vuelos2));
    }
}
