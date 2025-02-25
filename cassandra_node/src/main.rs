#![allow(dead_code)]

use std::{env, net::TcpStream, sync::Arc, thread};

use common::config::Config;
use handler_nodes::{start_gossip, start_flush, start_node_gossip_query_protocol, start_node_native_protocol};
use internal_protocol::InternalMessage;
use node::{GossipInformation, Node};

mod data_parser;
mod consistency;
mod consistent_hashing;
mod encrypted_table;
mod handler_nodes;
mod internal_protocol;
mod lock_test;
mod log;
mod native_protocol;
mod node;
mod query_parser;
mod replication_strategy;
mod query_builder;

fn connect_to_first_node(node: &Node, first_node_address: &str) {
    if let Ok(mut stream) = TcpStream::connect(first_node_address) {
        let gossip_table = node.get_gossip_table().unwrap_or_default();

        let gossip_messsage = InternalMessage::Gossip {
            opcode: 1,
            body: serde_json::to_string(&gossip_table).unwrap(),
        };

        if { gossip_messsage.write_to_stream(&mut stream) }.is_err() {
            println!("Error al enviar el mensaje de gossip:new node.");
        }

        match InternalMessage::deserialize_from_stream(&mut stream) {
            Ok(InternalMessage::Response { opcode: 0, body }) => {
                let gossip_table: Vec<GossipInformation> = serde_json::from_str(&body).unwrap();

                node.update_gossip_table(&gossip_table);
            }
            _ => {
                println!("Error al recibir el response de gossip: new node.");
            }
        }
    } else {
        println!("Error al conectar al primer nodo.");
    }
}

fn get_node() -> Result<Node, Box<(dyn std::error::Error)>> {
    let config = Config::new()?;

    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        return Err("No node id provided".into());
    }

    let Ok(node_id) = args[1].parse::<usize>() else {
        return Err("Failed to parse the string into a usize".into());
    };

    if node_id >= config.nodes.len() {
        return Err("Node id out of bounds".into());
    }
    
    // Cambia la ip de los nodos por parametro si se lo pasan
    // para debugear fuera de Docker
    let custom_address = if args.len() > 2 {
        Some(&args[2])
    } else {
        None
    };

    let node_config = &config.nodes[node_id];

    let node_ip = custom_address.unwrap_or(&node_config.address);
    let node = Node::new(
        &node_config.id,
        node_ip,
        node_config.public_port,
        node_config.private_port,
    );

    if node_id != 0 {
        let node_ip = custom_address.unwrap_or(&config.nodes[0].address);
        let private_port = config.nodes[0].private_port;
        let first_node_address = format!("{}:{}", node_ip, private_port);
        connect_to_first_node(&node, &first_node_address);
    } else {
        for i in 1..config.nodes.len() {
            let node_ip = custom_address.unwrap_or(&config.nodes[i].address);
            let private_port = config.nodes[i].private_port;
            let first_node_address = format!("{}:{}", node_ip, private_port);
            connect_to_first_node(&node, &first_node_address);
        }
    }

    Ok(node)
}

fn main() -> Result<(), Box<(dyn std::error::Error)>> {
    let node = get_node()?;

    let node = Arc::new(node);

    let node_clone_native = Arc::clone(&node);
    let native_handle: thread::JoinHandle<()> =
        thread::spawn(move || start_node_native_protocol(node_clone_native));

    let node_clone_gossip = Arc::clone(&node);
    let gossip_handle = thread::spawn(move || start_node_gossip_query_protocol(node_clone_gossip));

    let node_clone_start_gossip = Arc::clone(&node);
    start_gossip(node_clone_start_gossip, 1000);
    let node_clone_start_flush = Arc::clone(&node);
    start_flush(node_clone_start_flush, 10000);
    native_handle.join().unwrap();
    gossip_handle.join().unwrap();

    Ok(())
}
