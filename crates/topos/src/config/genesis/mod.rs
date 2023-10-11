use rlp::Rlp;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::{fs, path::PathBuf};

use serde_json::Value;
use topos_p2p::{Multiaddr, PeerId};
use topos_tce_transport::ValidatorId;
use tracing::info;

#[cfg(test)]
pub(crate) mod tests;

/// From the Edge format
pub struct Genesis {
    pub path: PathBuf,
    pub json: Value,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Error {
    ParseValidators,
}

impl Genesis {
    pub fn new(path: PathBuf) -> Self {
        info!("Reading subnet genesis file {}", path.display());
        let genesis_file = fs::File::open(&path).expect("opened file");

        let json: serde_json::Value =
            serde_json::from_reader(genesis_file).expect("genesis json parsed");

        Self { path, json }
    }

    // Considered as being the set of premined addresses for now
    // TODO: Parse properly genesis.extraData instead
    pub fn validator_count(&self) -> usize {
        self.json["genesis"]["alloc"]
            .as_object()
            .map_or(0, |v| v.len())
    }

    // TODO: parse directly with serde
    pub fn boot_peers(&self, port: Option<u16>) -> Vec<(PeerId, Multiaddr)> {
        match self.json["bootnodes"].as_array() {
            Some(v) => v
                .iter()
                .map(|bootnode| {
                    let (multiaddr, peerid) =
                        bootnode.as_str().unwrap().rsplit_once("/p2p/").unwrap();

                    // Extract the Edge port from the genesis file
                    let (multiaddr, edge_port) = multiaddr.rsplit_once('/').unwrap();

                    // Use the given port instead if any
                    let port = port.map_or(edge_port.to_string(), |p| p.to_string());

                    let multiaddr = format!("{multiaddr}/{port}");
                    (peerid.parse().unwrap(), multiaddr.parse().unwrap())
                })
                .collect::<Vec<_>>(),
            None => Vec::default(),
        }
    }

    /// Parse the validators from the `extraData` field of the genesis file.
    /// The `extraData` is padded with 32 bytes, and the validators are RLP encoded.
    /// Each validator is 20 bytes, with a SEAL at the end of the whole list (8 bytes)
    pub fn validators(&self) -> HashSet<ValidatorId> {
        let extra_data = self.json["genesis"]["extraData"]
            .as_str()
            .expect("The extraData field must be present. Bad genesis file?")
            .to_string();

        // Define constants for the prefix size and validator size
        const VANITY_SIZE: usize = 32;

        // Remove the "0x" prefix from the hex string
        let hex_string = &extra_data[2..];

        // Convert the hex string to bytes
        let bytes = hex::decode(hex_string).expect("Failed to decode hex string");

        // Slice the bytes to get the validators data
        let validators_data = &bytes[VANITY_SIZE..];

        // Create an Rlp object from the validators data
        let rlp = Rlp::new(validators_data);

        // Get the first Rlp item (index 0) and iterate over its items
        let first_item = rlp.at(0).expect("Failed to get first RLP item");
        let item_count = first_item
            .item_count()
            .expect("Validators must be an RLP list. Bad genesis file?");
        first_item.into_iter().fold(
            HashSet::with_capacity(item_count),
            |mut validator_public_keys, validator_rlp| {
                if let Ok(public_key) = validator_rlp.data() {
                    let address = format!("0x{}", hex::encode(&public_key[1..=20]));
                    validator_public_keys.insert(
                        ValidatorId::try_from(address.as_str()).unwrap_or_else(|error| {
                            panic!("Failed to convert address to ValidatorId: {:?}", error)
                        }),
                    );
                }
                validator_public_keys
            },
        )
    }
}
