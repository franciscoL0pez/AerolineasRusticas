use serde::de::DeserializeOwned;
use std::{fs::read_to_string, io};

use crate::models::{airplane::Airplane, airport::Airport};

const CONFIG_PATH: &str = "Config.toml"; // ahora este en el root del proyecto

#[derive(Debug, serde::Deserialize, Clone)]
pub struct NodeConfig {
    pub id: String,
    pub address: String,
    pub private_port: u16,
    pub public_port: u16,
}

#[derive(Debug, serde::Deserialize, Clone)]
pub struct UiConfig {
    pub gatherer: String,
    pub map_path: String,
    pub status_update_interval_in_ms: u64,
    pub tracking_update_interval_in_ms: u64,
}

#[derive(Debug, serde::Deserialize, Clone)]
pub struct Config {
    pub replication_factor: u64,
    pub simulation_thread_sleep_ms: u64,
    pub nodes_gateway_address: String,
    pub ui: UiConfig,
    pub nodes: Vec<NodeConfig>,
    pub airports: Vec<Airport>,
    pub airplanes: Vec<Airplane>,
}

impl Config {
    pub fn new() -> io::Result<Self> {
        deserialize_toml(CONFIG_PATH)
    }
}

fn deserialize_toml<T: DeserializeOwned>(path: &str) -> io::Result<T> {
    toml::from_str(&read_to_string(path)?)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/*
fn calculate_range(n: usize, number_of_nodes: usize) -> (u16, u16) {
    let range_size = u16::MAX / number_of_nodes as u16;
    let start = n as u16 * range_size;
    match n {
        n if n >= number_of_nodes - 1 => (start, u16::MAX), // ultimo
        _ => (start, (n as u16 + 1) * range_size - 1),
    }
}*/

pub fn gather_public_addresses(config: &Config) -> Vec<String> {
    let address = &config.nodes_gateway_address;
    config
        .nodes
        .iter()
        .map(|node| format!("{}:{}", address, node.public_port))
        .collect()
}
