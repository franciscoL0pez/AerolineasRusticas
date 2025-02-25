use crate::node::GossipInformation;
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone)]
pub struct ConsistentHash;

impl ConsistentHash {
    pub fn new() -> Self {
        ConsistentHash
    }

    /// Hashes a vector of partition keys.
    /// 
    /// #Parameters
    /// - `partition_keys`: Vector of partition keys.
    /// 
    pub fn hash_vector(&self, partition_keys: &Vec<String>) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        format!("{:?}", partition_keys).hash(&mut hasher);
        hasher.finish()
    }

    /// Gets the node id for a vector of partition_keys.
    /// 
    /// #Parameters
    /// - `partition_keys`: Vector of partition keys.
    /// - `gossip_table`: Contains gossip information of nodes.
    /// - `offset`: usize of n next nodes.
    /// 
    /// #Returns
    /// Node id according to the partition keys and offset.
    pub fn get_node_id(
        &self,
        partition_keys: &Vec<String>,
        gossip_table: &[GossipInformation],
        offset: usize,
    ) -> Result<String, String> {
        let num_nodes = gossip_table.len();
        let range_len = u64::MAX / num_nodes as u64;
        let hashed = self.hash_vector(partition_keys);
        for i in 0..gossip_table.len() {
            if hashed <= (i as u64 + 1) * range_len {
                if i + offset < num_nodes {
                    return Ok(gossip_table[i + offset].node_id.clone());
                } else {
                    return Ok(gossip_table[i + offset - num_nodes].node_id.clone());
                }
            }
        }
        Err("Error hashing partition keys to get node".to_string())
    }
}

impl Default for ConsistentHash {
    fn default() -> Self {
        Self::new()
    }
}
