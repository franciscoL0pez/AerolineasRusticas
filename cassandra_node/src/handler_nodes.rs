use super::node::Node;
use crate::internal_protocol::InternalMessage;
use crate::native_protocol::handle_native_protocol_connection;
use std::io::Read;
use std::io::Write;
use std::net::TcpListener;

use crate::log::Logger;
use std::sync::Arc;

use std::thread;
use std::vec;
/// Binds a `TcpListener` to all network interfaces (0.0.0.0) on the specified port.
/// 
/// # Arguments
///
/// - `port`: The port number on which the server will listen.
/// - `protocol_type`: A string describing the protocol type (e.g., "native", "gossip").
///   This information is used for logging purposes.
///
/// # Returns
///
/// Returns a `TcpListener` bound to the specified port. If binding fails, the function
/// logs the error and exits the program.
///
fn listen_on_all_interfaces(port: u16, protocol_type: &str) -> TcpListener {
    let full_address = format!("0.0.0.0:{}", port); // 0.0.0.0 != localhost
    match TcpListener::bind(&full_address) {
        Ok(listener) => {
            println!("Escuchando {} protocol en {}", protocol_type, &full_address);
            listener
        }
        Err(e) => {
            eprintln!("Error al vincular el puerto: {:?}", e);
            std::process::exit(1);
        }
    }
}


/// Starts the Gossip Query Protocol listener for the node.
///
/// #Parameters
/// - `node`: The node that will handle the incoming messages.
/// 

pub fn start_node_gossip_query_protocol(node: Arc<Node>) {
    let port = node.get_port_gossip_query();
    let nodes_listener = listen_on_all_interfaces(port, "internal");

    let _ = Logger::new(node.get_id());

    for stream in nodes_listener.incoming() {
        match stream {
            Ok(mut stream) => {
             
                InternalMessage::deserialize_from_stream(&mut stream)
                    .map(|message| node.receive_internal_message(&message))
                    .map(|response| match response {
                        Ok(response) => {
                            let result = InternalMessage::Response {
                                opcode: 0,
                                body: response,
                            }
                            .write_to_stream(&mut stream);
                            if let Err(e) = result {
                                eprintln!("Error al escribir en el stream: {}", e);
                            }
                        }
                        Err(e) => {
                            let result = InternalMessage::Response { opcode: 1, body: e }
                                .write_to_stream(&mut stream);
                            if let Err(e) = result {
                                eprintln!("Error al escribir en el stream: {}", e);
                            }
                        }
                    })
                    .unwrap_or_else(|e| {
                        eprintln!("Error al parsear el mensaje interno: {}", e);
                    });
            }
            Err(e) => {
                eprintln!("Error en la conexión: {}", e);
            }
        }
    }
}


/// Starts the native protocol listener for the node. 
///
/// #Parameters
/// - `node`: The node that will handle the incoming messages.
/// 
pub fn start_node_native_protocol(node: Arc<Node>) {
    let port = node.get_port_native_protocol();
    let client_listener = listen_on_all_interfaces(port, "native");

    for stream in client_listener.incoming() {
        match stream {
            Ok(stream) => {
                println!(
                    "Nueva conexión protocolo nativo -->: {}",
                    stream.peer_addr().unwrap()
                );
                let arc_clone = Arc::clone(&node);
                thread::spawn(move || handle_native_protocol_connection(stream, arc_clone));
            }
            Err(e) => eprintln!("Error en la conexión: {}", e),
        }
    }
}

/// Starts the gossip process for the given node at the specified interval.
/// 
/// #Parameters
/// - `node`: The node .
/// - `interval_in_ms`: The interval in milliseconds at which the gossip process will run.
/// 
pub fn start_gossip(node: Arc<Node>, interval_in_ms: u64) {
    thread::spawn(move || loop {
        {
            node.gossip(interval_in_ms);
        }
        thread::sleep(std::time::Duration::from_millis(interval_in_ms));
    });
}


pub fn start_flush(node: Arc<Node>, interval_in_ms: u64) {
    thread::spawn(move || loop {
        {
            node.flush();
        }
        thread::sleep(std::time::Duration::from_millis(interval_in_ms));
    });
}

/// Starts the native protocol listener for the node without using the native protocol.
/// 
/// #Parameters
/// - `node`: The node that will handle the incoming messages.
/// 
/// #Details
/// This function is used for testing purposes only.
/// 
pub fn start_node_native_protocol_without_native(node: Arc<Node>) {
    let port = node.get_port_native_protocol();
    let Ok(client_listener) = TcpListener::bind(format!("localhost:{}", port)) else {
        eprintln!("Error al vincular el puerto");
        return;
    };
    println!("Servidor native protocol escuchando {}", port);

    let mut current_keyspace_name = "".to_string();

    for stream in client_listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let mut buffer = vec![0; 8000];
                match stream.read(&mut buffer) {
                    Ok(bytes_read) => {
                        if bytes_read == 0 {
                            eprintln!("Cliente cerró la conexión.");
                            continue;
                        }
                        let query = String::from_utf8_lossy(&buffer[..bytes_read]).to_string();

                        let query_cloned = query.clone();
                        if query_cloned.contains("USE") {
                            
                            let query_parts: Vec<&str> = query_cloned.split_whitespace().collect();
                            if query_parts.len() != 2 {
                                eprintln!("Usage: USE <keyspace_name>;");
                                continue;
                            }
                            let keyspace_name: Vec<&str> = query_parts[1].split(';').collect();
                            if keyspace_name.len() == 1 {
                                eprintln!("USE query not ending with ';'");
                                continue;
                            } else {
                                current_keyspace_name = keyspace_name[0].to_string();
                                continue;
                            }
                        }

                        if current_keyspace_name.is_empty() && !query.contains("CREATE KEYSPACE") {
                            eprintln!("No se especificó un keyspace.");
                            continue;
                        }

                        println!("Mando al nodo el query: {}", query);
                        match node.resend_query_as_internal_message_mock_not_native(
                            &query,
                            &current_keyspace_name,
                        ) {
                            Ok(response) => {
                                match stream.write_all(response.to_string().as_bytes()) {
                                    Ok(_) => {}
                                    Err(e) => {
                                        eprintln!("Error al escribir en el stream: {}", e);
                                    }
                                };
                            }
                            Err(response) => {
                                stream
                                    .write_all(format!("{:?}", response).as_bytes())
                                    .unwrap();
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Error al leer del stream: {}", e);
                    }
                }
            }
            Err(e) => {
                eprintln!("Error en la conexión: {}", e);
            }
        }
    }
}

// Para estos test usamos los metodos de sin native protocol (para poder aislar las pruebas)
#[cfg(test)]
mod test_handler {

    use super::*;
    use common::client_manager::ClientManager;
    use crate::node::GossipInformation;
    use std::collections::HashMap;
    use std::net::TcpStream;
    use std::vec;

    #[test]
    fn creo_un_nodo_y_inserto_un_dato() {
        let nodo1 = Arc::new(Node::new("Nodo1", "localhost", 10011, 10012));

        let node1_clone = Arc::clone(&nodo1);
        let node2_clone = Arc::clone(&nodo1);

        thread::spawn(move || start_node_native_protocol_without_native(node1_clone));

        thread::spawn(move || start_node_gossip_query_protocol(node2_clone));
        thread::sleep(std::time::Duration::from_millis(1000));

        let mut queries = vec![];

        queries.push("CREATE KEYSPACE keyspace1 WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        queries.push("USE keyspace1;");
        queries.push("CREATE TABLE vuelos (id_flight INT, from_city TEXT, to_city TEXT, departure_time TEXT, PRIMARY KEY ((from_city), departure_time));");
        queries.push("INSERT INTO vuelos (id_flight, from_city, to_city, departure_time) VALUES (1010, 'Rio', 'Catamarca', '22');");

        for query in queries {
            if let Ok(mut stream) = TcpStream::connect("localhost:10011") {
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
        values_vuelos1.insert("departure_time".to_string(), "22".to_string());

        let node1_1 = nodo1.clone();

        node1_1.show();

        let vector_rows = node1_1
            .get_table("keyspace1", "vuelos")
            .unwrap()
            .get_vector_of_rows();

        println!("Vector de rows: {:?}", vector_rows);

        assert!(vector_rows.contains(&values_vuelos1));
    }

    #[test]
    fn creo2_nodos_y_intercambian_datos_de_gossip() {
        let nodo1 = Arc::new(Node::new("Nodo1", "localhost", 10021, 10022));
        let nodo2 = Arc::new(Node::new("Nodo2", "localhost", 20021, 20022));

        let node1_clone = Arc::clone(&nodo1);
        let node1_clone2 = Arc::clone(&nodo1);

        thread::spawn(move || start_node_gossip_query_protocol(node1_clone2));
        thread::sleep(std::time::Duration::from_millis(1000));
        thread::spawn(move || start_node_native_protocol_without_native(node1_clone));

        let node2_clone = Arc::clone(&nodo2);
        thread::spawn(move || start_node_gossip_query_protocol(node2_clone));
        thread::sleep(std::time::Duration::from_millis(1000));

        if let Ok(mut stream) = TcpStream::connect("localhost:10022") {
            let gossip_table = match nodo2.get_gossip_table() {
                Ok(gossip_table) => gossip_table,
                Err(e) => {
                    println!("Error al obtener la tabla de gossip del nodo2: {}", e);
                    return;
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
                    let start = gossip_table_response.find('[').unwrap_or(0);
                    let gossip_table: Vec<GossipInformation> =
                        serde_json::from_str(&gossip_table_response[start..]).unwrap();
                    nodo2.update_gossip_table(&gossip_table);
                }
                Err(e) => {
                    println!("Error al leer del stream o timeout: {}", e);
                }
            }
        } else {
            println!("Error al conectar al nodo1.");
        }

        thread::sleep(std::time::Duration::from_millis(1000));

        let gossip_table_nodo1 = match nodo1.get_gossip_table() {
            Ok(gossip_table) => gossip_table,
            Err(e) => {
                println!("Error al obtener la tabla de gossip del nodo1: {}", e);
                return;
            }
        };

        let gossip_table_nodo2 = match nodo2.get_gossip_table() {
            Ok(gossip_table) => gossip_table,
            Err(e) => {
                println!("Error al obtener la tabla de gossip del nodo2: {}", e);
                return;
            }
        };

        assert_eq!(gossip_table_nodo1.len(), gossip_table_nodo2.len());
    }

    #[test]
    fn creo_5_nodos_y_aplico_gossip() {
        let nodo1 = Arc::new(Node::new("Nodo1", "localhost", 10031, 10032));
        let nodo2 = Arc::new(Node::new("Nodo2", "localhost", 20031, 20032));
        let nodo3 = Arc::new(Node::new("Nodo3", "localhost", 30031, 30032));
        let nodo4 = Arc::new(Node::new("Nodo4", "localhost", 40031, 40032));
        let nodo5 = Arc::new(Node::new("Nodo5", "localhost", 50031, 50032));

        let node1_clone = Arc::clone(&nodo1);
        let node1_clone2 = Arc::clone(&nodo1);

        thread::spawn(move || start_node_native_protocol_without_native(node1_clone));
        thread::spawn(move || start_node_gossip_query_protocol(node1_clone2));

        let node2_clone = Arc::clone(&nodo2);
        thread::spawn(move || start_node_gossip_query_protocol(node2_clone));
        let node3_clone = Arc::clone(&nodo3);
        thread::spawn(move || start_node_gossip_query_protocol(node3_clone));
        let node4_clone = Arc::clone(&nodo4);
        thread::spawn(move || start_node_gossip_query_protocol(node4_clone));
        let node5_clone = Arc::clone(&nodo5);
        thread::spawn(move || start_node_gossip_query_protocol(node5_clone));

        thread::sleep(std::time::Duration::from_millis(1000));

        for node in [&nodo2, &nodo3, &nodo4, &nodo5] {
            if let Ok(mut stream) = TcpStream::connect("localhost:10032") {
                let gossip_table = match node.get_gossip_table() {
                    Ok(gossip_table) => gossip_table,
                    Err(e) => {
                        println!("Error al obtener la tabla de gossip del nodo2: {}", e);
                        return;
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
                        let start = gossip_table_response.find('[').unwrap_or(0);
                        let gossip_table: Vec<GossipInformation> =
                            serde_json::from_str(&gossip_table_response[start..]).unwrap();
                        node.update_gossip_table(&gossip_table);
                        println!("Llega al final");
                    }
                    Err(e) => {
                        println!("Error al leer del stream o timeout: {}", e);
                    }
                }
            } else {
                println!("Error al conectar al nodo1.");
            }
        }
        thread::sleep(std::time::Duration::from_millis(300));

        for node in [
            Arc::clone(&nodo1),
            Arc::clone(&nodo2),
            Arc::clone(&nodo3),
            Arc::clone(&nodo4),
            Arc::clone(&nodo5),
        ] {
            start_gossip(node, 1000);
        }
        println!("Espero rondas de gossip");
        thread::sleep(std::time::Duration::from_secs(5));

        assert_eq!(
            nodo1.get_gossip_table().unwrap().len(),
            nodo2.get_gossip_table().unwrap().len()
        );
        assert_eq!(
            nodo2.get_gossip_table().unwrap().len(),
            nodo3.get_gossip_table().unwrap().len()
        );
        assert_eq!(
            nodo4.get_gossip_table().unwrap().len(),
            nodo5.get_gossip_table().unwrap().len()
        );
    }

    #[test]
    fn realizo_un_insert_con_3_nodos() {
        let nodo1 = Arc::new(Node::new("Nodo1", "localhost", 10041, 10042));
        let nodo2 = Arc::new(Node::new("Nodo2", "localhost", 20041, 20042));
        let nodo3 = Arc::new(Node::new("Nodo3", "localhost", 30041, 30042));

        let node1_clone = Arc::clone(&nodo1);
        let node1_clone2 = Arc::clone(&nodo1);

        thread::spawn(move || start_node_native_protocol_without_native(node1_clone));
        thread::spawn(move || start_node_gossip_query_protocol(node1_clone2));

        let node2_clone = Arc::clone(&nodo2);
        thread::spawn(move || start_node_gossip_query_protocol(node2_clone));

        let node3_clone = Arc::clone(&nodo3);
        let node3_2clone = Arc::clone(&nodo3);
        thread::spawn(move || start_node_gossip_query_protocol(node3_clone));
        thread::spawn(move || start_node_native_protocol_without_native(node3_2clone));

        thread::sleep(std::time::Duration::from_millis(1000));

        for node in vec![&nodo2, &nodo3].into_iter() {
            if let Ok(mut stream) = TcpStream::connect("localhost:10042") {
                let gossip_table = match node.get_gossip_table() {
                    Ok(gossip_table) => gossip_table,
                    Err(e) => {
                        println!("Error al obtener la tabla de gossip del nodo2: {}", e);
                        return;
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
        thread::sleep(std::time::Duration::from_millis(3000));

        for node in [Arc::clone(&nodo1), Arc::clone(&nodo2), Arc::clone(&nodo3)] {
            start_gossip(node, 1000);
        }
        println!("Espero 3 segundos");
        thread::sleep(std::time::Duration::from_secs(3));

        let mut queries = vec![];
        queries.push("CREATE KEYSPACE keyspace1 WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };");
        queries.push("USE keyspace1;");
        queries.push("CREATE TABLE vuelos (id_flight INT, from_city TEXT, to_city TEXT, departure_time TEXT, PRIMARY KEY ((from_city), departure_time));");
        queries.push("INSERT INTO vuelos (id_flight, from_city, to_city, departure_time) VALUES (1010, 'Rio', 'Catamarca', '22');");

        for query in queries {
            if let Ok(mut stream) = TcpStream::connect("localhost:30041") {
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
        values_vuelos1.insert("departure_time".to_string(), "22".to_string());

        let node1_1 = nodo1.clone();

        assert!(node1_1
            .get_table("keyspace1", "vuelos")
            .unwrap()
            .get_vector_of_rows()
            .contains(&values_vuelos1));
    }

    #[test]
    fn realizo_varios_insert_con_datos_similares_en_3_nodos_y_piso_los_datos() {
        let nodo1 = Arc::new(Node::new("Nodo1", "localhost", 10051, 10052));
        let nodo2 = Arc::new(Node::new("Nodo2", "localhost", 20051, 20052));
        let nodo3 = Arc::new(Node::new("Nodo3", "localhost", 30051, 30052));

        let node1_clone = Arc::clone(&nodo1);
        thread::spawn(move || start_node_gossip_query_protocol(node1_clone));
        let node2_clone = Arc::clone(&nodo2);
        thread::spawn(move || start_node_gossip_query_protocol(node2_clone));

        let node3_clone = Arc::clone(&nodo3);
        let node3_clone_native = Arc::clone(&nodo3);
        thread::spawn(move || start_node_native_protocol_without_native(node3_clone_native));

        thread::spawn(move || start_node_gossip_query_protocol(node3_clone));

        thread::sleep(std::time::Duration::from_millis(1000));

        for node in vec![&nodo2, &nodo3].into_iter() {
            if let Ok(mut stream) = TcpStream::connect("localhost:10052") {
                let gossip_table = match node.get_gossip_table() {
                    Ok(gossip_table) => gossip_table,
                    Err(e) => {
                        println!("Error al obtener la tabla de gossip del nodo2: {}", e);
                        return;
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
        thread::sleep(std::time::Duration::from_millis(300));

        for node in [Arc::clone(&nodo1), Arc::clone(&nodo2), Arc::clone(&nodo3)] {
            start_gossip(node, 1000);
        }
        println!("Espero 3 segundos");
        thread::sleep(std::time::Duration::from_secs(3));

        //Ojo que aca la primarykey = (from_city, departure_time) = ()
        let mut queries = vec![];
        queries.push("CREATE KEYSPACE keyspace1 WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        queries.push("USE keyspace1;");
        queries.push("CREATE TABLE vuelos (id_flight INT, from_city TEXT, to_city TEXT, departure_time TEXT, PRIMARY KEY ((from_city), departure_time));");
        queries.push("INSERT INTO vuelos (id_flight, from_city, to_city, departure_time) VALUES (1010, 'Rio', 'Catamarca', '22');");
        queries.push("INSERT INTO vuelos (id_flight, from_city, to_city, departure_time) VALUES (1010, 'Rio', 'Jujuy', '22');");

        for query in queries {
            if let Ok(mut stream) = TcpStream::connect("localhost:30051") {
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
        values_vuelos1.insert("departure_time".to_string(), "22".to_string());

        let mut values_vuelos2 = HashMap::new();
        values_vuelos2.insert("id_flight".to_string(), "1010".to_string());
        values_vuelos2.insert("from_city".to_string(), "Rio".to_string());
        values_vuelos2.insert("to_city".to_string(), "Jujuy".to_string());
        values_vuelos2.insert("departure_time".to_string(), "22".to_string());

        let node1_1 = nodo1.clone();

        //Como el realice un insert con mismo id y partition key y clustering key se pisa el valor
        assert!(!node1_1
            .get_table("keyspace1", "vuelos")
            .unwrap()
            .get_vector_of_rows()
            .contains(&values_vuelos1));

        assert!(node1_1
            .get_table("keyspace1", "vuelos")
            .unwrap()
            .get_vector_of_rows()
            .contains(&values_vuelos2));

        node1_1.show();
    }

    #[test]
    fn realizo_varios_inserts_y_compurebo_los_datos_con_un_select() {
        let nodo1 = Arc::new(Node::new("Nodo1", "localhost", 10061, 10062));
        let nodo2 = Arc::new(Node::new("Nodo2", "localhost", 20061, 20062));
        let nodo3 = Arc::new(Node::new("Nodo3", "localhost", 30061, 30062));

        let node1_clone = Arc::clone(&nodo1);

        thread::spawn(move || start_node_gossip_query_protocol(node1_clone));
        let node2_clone = Arc::clone(&nodo2);
        thread::spawn(move || start_node_gossip_query_protocol(node2_clone));

        let node3_clone = Arc::clone(&nodo3);
        let node3_clone_native = Arc::clone(&nodo3);
        thread::spawn(move || start_node_native_protocol_without_native(node3_clone_native));
        thread::spawn(move || start_node_gossip_query_protocol(node3_clone));

        thread::sleep(std::time::Duration::from_millis(1000));

        for node in vec![&nodo2, &nodo3].into_iter() {
            if let Ok(mut stream) = TcpStream::connect("localhost:10062") {
                let gossip_table = match node.get_gossip_table() {
                    Ok(gossip_table) => gossip_table,
                    Err(e) => {
                        println!("Error al obtener la tabla de gossip del nodo2: {}", e);
                        return;
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

        for node in [Arc::clone(&nodo1), Arc::clone(&nodo2), Arc::clone(&nodo3)] {
            start_gossip(node, 1000);
        }

        println!("Espero 3 segundos");
        thread::sleep(std::time::Duration::from_secs(5));

        //Ojo que aca la primarykey = (from_city, departure_time) = ()
        //mirar comunicacion entre nodos por que creo que falla cuando R.F = 1 (no esta agarrando bien los nodos)
        let mut queries = vec![];
        queries.push("CREATE KEYSPACE keyspace1 WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1};");
        queries.push("USE keyspace1;");
        queries.push("CREATE TABLE vuelos (id_flight INT, from_city TEXT, to_city TEXT, departure_time TEXT, PRIMARY KEY ((id_flight), from_city));");
        queries.push("INSERT INTO vuelos (id_flight, from_city, to_city, departure_time) VALUES (1010, 'Rio', 'Catamarca', '21');");
        queries.push("INSERT INTO vuelos (id_flight, from_city, to_city, departure_time) VALUES (1011, 'Bariloche', 'Chubut', '22');");

        for query in queries {
            if let Ok(mut stream) = TcpStream::connect("localhost:30061") {
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

        let mut select_values: Vec<HashMap<String, String>> = vec![];
        let query = "SELECT * FROM vuelos WHERE id_flight = '1010';";
        if let Ok(mut stream) = TcpStream::connect("localhost:30061") {
            stream.write_all(query.as_bytes()).unwrap();
            println!("Envio la query");
            let mut buffer = vec![0; 8000]; // Inicializa un buffer vacío
            stream.read(&mut buffer).unwrap();
            let response = String::from_utf8_lossy(&buffer).to_string();
            let mut string = String::new();
            for c in response.chars() {
                if c != '\0' {
                    string.push(c);
                }
            }
            select_values = serde_json::from_str(&string).unwrap();
        } else {
            println!("Error al conectar al nodo1.");
        }
        thread::sleep(std::time::Duration::from_millis(2000));

        assert!(select_values.contains(&values_vuelos1));

        let mut values_vuelos2 = HashMap::new();
        values_vuelos2.insert("id_flight".to_string(), "1011".to_string());
        values_vuelos2.insert("from_city".to_string(), "Bariloche".to_string());
        values_vuelos2.insert("to_city".to_string(), "Chubut".to_string());
        values_vuelos2.insert("departure_time".to_string(), "22".to_string());

        let mut select_values: Vec<HashMap<String, String>> = vec![];
        let query = "SELECT * FROM vuelos WHERE id_flight = '1011';";
        if let Ok(mut stream) = TcpStream::connect("localhost:30061") {
            stream.write_all(query.as_bytes()).unwrap();
            println!("Envio la query");
            let mut buffer = vec![0; 8000]; // Inicializa un buffer vacío
            stream.read(&mut buffer).unwrap();
            let response = String::from_utf8_lossy(&buffer).to_string();
            let mut string = String::new();
            for c in response.chars() {
                if c != '\0' {
                    string.push(c);
                }
            }
            select_values = serde_json::from_str(&string).unwrap();
        } else {
            println!("Error al conectar al nodo1.");
        }
        thread::sleep(std::time::Duration::from_millis(2000));

        assert!(select_values.contains(&values_vuelos2));
    }

    #[test]
    fn test_read_repair() {
        let node1 = Arc::new(Node::new("Node1", "localhost", 10071, 10072));
        let node1_clone1 = Arc::clone(&node1);
        let node1_clone2 = Arc::clone(&node1);
        thread::spawn(move || start_node_native_protocol(node1_clone1));
        thread::spawn(move || start_node_gossip_query_protocol(node1_clone2));
        let node1_clone3 = Arc::clone(&node1);
        start_gossip(node1_clone3, 1000);

        let node2 = Arc::new(Node::new("Node2", "localhost", 20071, 20072));
        let node2_clone1 = Arc::clone(&node2);
        let node2_clone2 = Arc::clone(&node2);

        thread::sleep(std::time::Duration::from_millis(1000));

        if let Ok(mut stream) = TcpStream::connect("localhost:10072") {
            let gossip_table = match node2.get_gossip_table() {
                Ok(gossip_table) => gossip_table,
                Err(e) => {
                    println!("Error al obtener la tabla de gossip del nodo2: {}", e);
                    return;
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

        let node3 = Arc::new(Node::new("Node3", "localhost", 30071, 30072));
        let node3_clone1 = Arc::clone(&node3);
        let node3_clone2 = Arc::clone(&node3);

        if let Ok(mut stream) = TcpStream::connect("localhost:10072") {
            let gossip_table = match node3.get_gossip_table() {
                Ok(gossip_table) => gossip_table,
                Err(e) => {
                    println!("Error al obtener la tabla de gossip del nodo2: {}", e);
                    return;
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
                    let gossip_table: Vec<GossipInformation> =
                        serde_json::from_str(&gossip_table_response).unwrap();
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
        thread::sleep(std::time::Duration::from_millis(3000));

        let addresses: Vec<String> =
            vec!["localhost:10071".to_string(), "localhost:20071".to_string()];
        let mut client_manager = match ClientManager::new(&addresses) {
            Ok(client_manager) => client_manager,
            Err(e) => {
                eprintln!("Error al crear el client manager: {:?}", e);
                return;
            }
        };

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
                    timestamp TIMESTAMP,
                    PRIMARY KEY ((origin_airport_id), departure_time)
                );"
                .to_string(),
                "ONE",
            )
            .expect("Error al ejecutar create table");

        thread::sleep(std::time::Duration::from_millis(1000));
        client_manager.query("INSERT INTO flight_status_by_origin
                (flight_id, origin_airport_id, destination_airport_id, departure_time, arrival_time, status)
                VALUES (10001, 20, 8888, '2024-09-27 09:00:00', '2024-09-27 18:00:00', 'on_time');".to_string(),"QUORUM")
                .expect("Error al ejecutar insert");

        thread::sleep(std::time::Duration::from_millis(3000));

        let values: HashMap<String, String> = vec![
            ("flight_id".to_string(), "10001".to_string()),
            ("origin_airport_id".to_string(), "20".to_string()),
            ("destination_airport_id".to_string(), "8888".to_string()),
            (
                "departure_time".to_string(),
                "2024-09-27 09:00:00".to_string(),
            ),
            (
                "arrival_time".to_string(),
                "2024-09-27 18:00:00".to_string(),
            ),
            ("status".to_string(), "new status".to_string()),
            (
                "_timestamp".to_string(),
                "2030-01-01 12:00:00".to_string(),
            )
        ]
        .into_iter()
        .collect();

        let keyspace_name = "flights_keyspace".to_string();
        let table_name = "flight_status_by_origin".to_string();

        let _ = node3.insert_row(&keyspace_name, &table_name, values.clone());

        match client_manager.query(
            "SELECT * FROM flight_status_by_origin WHERE origin_airport_id = '20' ;".to_string(),
            "ALL",
        ) {
            Ok(response) => {
                println!("Response: {:?}", response);
            }
            Err(e) => {
                eprintln!("Error al ejecutar la query: {:?}", e);
            }
        }

        thread::sleep(std::time::Duration::from_millis(1000));

        thread::sleep(std::time::Duration::from_millis(1000));

        //Deberian tener todos le vector actualizado

        // print vector de rows de cada nodo
        for node in [&node1, &node2, &node3] {
            println!("Node: {}", node.get_id());
            for row in node
                .get_table(&keyspace_name, &table_name)
                .unwrap()
                .get_vector_of_rows()
            {
                println!("{:?}", row);
            }
        }

        for (key, value) in &values {
            assert!(node2
                .get_table(&keyspace_name, &table_name)
                .unwrap()
                .get_vector_of_rows()
                .iter()
                .any(|row| row.get(key).map(|v| v == value).unwrap_or(false)), 
                "node2: El valor '{value}' para '{key}' no coincide con el esperado."
            );
        
            assert!(node1
                .get_table(&keyspace_name, &table_name)
                .unwrap()
                .get_vector_of_rows()
                .iter()
                .any(|row| row.get(key).map(|v| v == value).unwrap_or(false)),
                "node1: El valor '{value}' para '{key}' no coincide con el esperado."
            );
        }
    }

    #[test]
    fn test_update() {
        let node1 = Arc::new(Node::new("Node1", "localhost", 10081, 10082));
        let node1_clone1 = Arc::clone(&node1);
        let node1_clone2 = Arc::clone(&node1);
        thread::spawn(move || start_node_native_protocol(node1_clone1));
        thread::spawn(move || start_node_gossip_query_protocol(node1_clone2));
        let node1_clone3 = Arc::clone(&node1);
        start_gossip(node1_clone3, 1000);

        let node2 = Arc::new(Node::new("Node2", "localhost", 20081, 20082));
        let node2_clone1 = Arc::clone(&node2);
        let node2_clone2 = Arc::clone(&node2);

        thread::sleep(std::time::Duration::from_millis(1000));

        if let Ok(mut stream) = TcpStream::connect("localhost:10082") {
            let gossip_table = match node2.get_gossip_table() {
                Ok(gossip_table) => gossip_table,
                Err(e) => {
                    println!("Error al obtener la tabla de gossip del nodo2: {}", e);
                    return;
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

        let node3 = Arc::new(Node::new("Node3", "localhost", 30081, 30082));
        let node3_clone1 = Arc::clone(&node3);
        let node3_clone2 = Arc::clone(&node3);

        if let Ok(mut stream) = TcpStream::connect("localhost:10082") {
            let gossip_table = match node3.get_gossip_table() {
                Ok(gossip_table) => gossip_table,
                Err(e) => {
                    println!("Error al obtener la tabla de gossip del nodo2: {}", e);
                    return;
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
                    let gossip_table: Vec<GossipInformation> =
                        serde_json::from_str(&gossip_table_response).unwrap();
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
        thread::sleep(std::time::Duration::from_millis(3000));

        let addresses: Vec<String> =
            vec!["localhost:10081".to_string(), "localhost:20081".to_string()];
        let mut client_manager = match ClientManager::new(&addresses) {
            Ok(client_manager) => client_manager,
            Err(e) => {
                eprintln!("Error al crear el client manager: {:?}", e);
                return;
            }
        };

        client_manager
            .query(
                "CREATE KEYSPACE flights_keyspace
                WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 3};"
                    .to_string(),
                "ALL",
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
                    timestamp TIMESTAMP,
                    PRIMARY KEY ((origin_airport_id), departure_time)
                );"
                .to_string(),
                "ONE",
            )
            .expect("Error al ejecutar create table");

        thread::sleep(std::time::Duration::from_millis(1000));
        client_manager.query("INSERT INTO flight_status_by_origin
                (flight_id, origin_airport_id, destination_airport_id, departure_time, arrival_time, status, timestamp)
                VALUES (10001, 20, 8888, '2024-09-27 09:00:00', '2024-09-27 18:00:00', 'on_time', '2024-01-01 00:00:00');".to_string(),"QUORUM")
                .expect("Error al ejecutar insert");

        thread::sleep(std::time::Duration::from_millis(1000));
        client_manager
            .query(
                "UPDATE flight_status_by_origin
                SET status = 'delayed' WHERE flight_id = 10001;"
                    .to_string(),
                "QUORUM",
            )
            .expect("Error al ejecutar update");

        thread::sleep(std::time::Duration::from_millis(1000));

        let values: HashMap<String, String> = vec![
            ("flight_id".to_string(), "10001".to_string()),
            ("origin_airport_id".to_string(), "20".to_string()),
            ("destination_airport_id".to_string(), "8888".to_string()),
            (
                "departure_time".to_string(),
                "2024-09-27 09:00:00".to_string(),
            ),
            (
                "arrival_time".to_string(),
                "2024-09-27 18:00:00".to_string(),
            ),
            ("status".to_string(), "delayed".to_string()),
        ]
        .into_iter()
        .collect();

        let keyspace_name = "flights_keyspace".to_string();
        let table_name = "flight_status_by_origin".to_string();

        println!(
            "{:?}",
            node2
                .get_table(&keyspace_name, &table_name)
                .unwrap()
                .get_vector_of_rows()
        );
        
        for (key, value) in &values {
            assert!(node2
                .get_table(&keyspace_name, &table_name)
                .unwrap()
                .get_vector_of_rows()
                .iter()
                .any(|row| row.get(key).map(|v| v == value).unwrap_or(false)), 
                "node2: El valor '{value}' para '{key}' no coincide con el esperado."
            );
        
            assert!(node1
                .get_table(&keyspace_name, &table_name)
                .unwrap()
                .get_vector_of_rows()
                .iter()
                .any(|row| row.get(key).map(|v| v == value).unwrap_or(false)),
                "node1: El valor '{value}' para '{key}' no coincide con el esperado."
            );

            assert!(node3
                .get_table(&keyspace_name, &table_name)
                .unwrap()
                .get_vector_of_rows()
                .iter()
                .any(|row| row.get(key).map(|v| v == value).unwrap_or(false)),
                "node3: El valor '{value}' para '{key}' no coincide con el esperado."
            );
        }
    }

    #[test]
    fn test_delete() {
        let node1 = Arc::new(Node::new("Node1", "localhost", 10091, 10092));
        let node1_clone1 = Arc::clone(&node1);
        let node1_clone2 = Arc::clone(&node1);
        thread::spawn(move || start_node_native_protocol(node1_clone1));
        thread::spawn(move || start_node_gossip_query_protocol(node1_clone2));
        let node1_clone3 = Arc::clone(&node1);
        start_gossip(node1_clone3, 1000);

        let node2 = Arc::new(Node::new("Node2", "localhost", 20091, 20092));
        let node2_clone1 = Arc::clone(&node2);
        let node2_clone2 = Arc::clone(&node2);

        thread::sleep(std::time::Duration::from_millis(1000));

        if let Ok(mut stream) = TcpStream::connect("localhost:10092") {
            let gossip_table = match node2.get_gossip_table() {
                Ok(gossip_table) => gossip_table,
                Err(e) => {
                    println!("Error al obtener la tabla de gossip del nodo2: {}", e);
                    return;
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

        let node3 = Arc::new(Node::new("Node3", "localhost", 30091, 30092));
        let node3_clone1 = Arc::clone(&node3);
        let node3_clone2 = Arc::clone(&node3);

        if let Ok(mut stream) = TcpStream::connect("localhost:10092") {
            let gossip_table = match node3.get_gossip_table() {
                Ok(gossip_table) => gossip_table,
                Err(e) => {
                    println!("Error al obtener la tabla de gossip del nodo2: {}", e);
                    return;
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
                    let gossip_table: Vec<GossipInformation> =
                        serde_json::from_str(&gossip_table_response).unwrap();
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
        thread::sleep(std::time::Duration::from_millis(3000));

        let addresses: Vec<String> =
            vec!["localhost:10091".to_string(), "localhost:20091".to_string()];
        let mut client_manager = match ClientManager::new(&addresses) {
            Ok(client_manager) => client_manager,
            Err(e) => {
                eprintln!("Error al crear el client manager: {:?}", e);
                return;
            }
        };

        client_manager
            .query(
                "CREATE KEYSPACE flights_keyspace
                WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 3};"
                    .to_string(),
                "ALL",
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
                    timestamp TIMESTAMP,
                    PRIMARY KEY ((origin_airport_id), departure_time)
                );"
                .to_string(),
                "ONE",
            )
            .expect("Error al ejecutar create table");

        thread::sleep(std::time::Duration::from_millis(1000));
        client_manager.query("INSERT INTO flight_status_by_origin
                (flight_id, origin_airport_id, destination_airport_id, departure_time, arrival_time, status, timestamp)
                VALUES (10001, 20, 8888, '2024-09-27 09:00:00', '2024-09-27 18:00:00', 'on_time', '2024-01-01 00:00:00');".to_string(),"QUORUM")
                .expect("Error al ejecutar insert");

        thread::sleep(std::time::Duration::from_millis(1000));
        client_manager
            .query(
                "DELETE FROM flight_status_by_origin WHERE flight_id = 10001;".to_string(),
                "QUORUM",
            )
            .expect("Error al ejecutar update");

        thread::sleep(std::time::Duration::from_millis(1000));

        let values: HashMap<String, String> = vec![
            ("flight_id".to_string(), "10001".to_string()),
            ("origin_airport_id".to_string(), "20".to_string()),
            ("destination_airport_id".to_string(), "8888".to_string()),
            (
                "departure_time".to_string(),
                "2024-09-27 09:00:00".to_string(),
            ),
            (
                "arrival_time".to_string(),
                "2024-09-27 18:00:00".to_string(),
            ),
            ("status".to_string(), "on_time".to_string()),
            ("timestamp".to_string(), "2024-01-01 00:00:00".to_string()),
        ]
        .into_iter()
        .collect();

        let keyspace_name = "flights_keyspace".to_string();
        let table_name = "flight_status_by_origin".to_string();

        println!(
            "{:?}",
            node2
                .get_table(&keyspace_name, &table_name)
                .unwrap()
                .get_vector_of_rows()
        );

        assert!(!node3
            .get_table(&keyspace_name, &table_name)
            .unwrap()
            .get_vector_of_rows()
            .contains(&values));

        assert!(!node2
            .get_table(&keyspace_name, &table_name)
            .unwrap()
            .get_vector_of_rows()
            .contains(&values));

        assert!(!node1
            .get_table(&keyspace_name, &table_name)
            .unwrap()
            .get_vector_of_rows()
            .contains(&values));
    }
}
