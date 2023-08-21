#![allow(unused_variables)]
#![allow(unused_imports)]
mod behaviour;
mod client;
mod command;
pub mod config;
pub mod constant;
pub mod error;
mod event;
mod runtime;
#[cfg(test)]
mod tests;

use std::str::FromStr;

pub(crate) use behaviour::Behaviour;
pub use client::Client;
pub use client::RetryPolicy;
pub(crate) use command::Command;
pub use command::NotReadyMessage;
pub use event::Event;
use libp2p::identity;
pub use libp2p::Multiaddr;
pub use libp2p::PeerId;
pub use runtime::Runtime;

use topos_crypto::keys;

pub mod network;

pub const TOPOS_GOSSIP: &str = "topos_gossip";
pub const TOPOS_ECHO: &str = "topos_echo";
pub const TOPOS_READY: &str = "topos_ready";

pub mod utils {
    use libp2p::identity;

    /// build peer_id keys, generate for now - either from the seed or purely random one
    pub fn local_key_pair(secret_key_seed: Option<u8>) -> identity::Keypair {
        // todo: load from protobuf encoded|base64 encoded config.local_key_pair
        match secret_key_seed {
            Some(seed) => {
                let mut bytes = [0u8; 32];
                bytes[0] = seed;
                identity::Keypair::ed25519_from_bytes(bytes).expect("Invalid keypair")
            }
            None => identity::Keypair::generate_ed25519(),
        }
    }

    pub fn local_key_pair_from_slice(slice: &[u8]) -> identity::Keypair {
        // todo: load from protobuf encoded|base64 encoded config.local_key_pair
        let mut bytes = [0u8; 32];
        if slice.len() <= 32 {
            bytes[..slice.len()].clone_from_slice(slice);
        } else {
            bytes.clone_from_slice(&slice[..32]);
        }

        identity::Keypair::ed25519_from_bytes(bytes).expect("Invalid keypair")
    }

    pub fn keypair_from_protobuf_encoding(priv_key: &[u8]) -> identity::Keypair {
        identity::Keypair::from_protobuf_encoding(priv_key).expect("Invalid keypair retrieval")
    }
}

#[test]
pub fn generate_from_secp256k1() {
    // Key living in the AWS SM or FS at libp2p/libp2p.key
    let edge_dec_privkey =
        hex::decode("08021220eb5ce97bd3e7729ac4ab077b83881426cebf19e58a9d9760d1cedfc53d772d6c")
            .expect("Failed to hex decode");

    let edge_peerid =
        PeerId::from_str("16Uiu2HAkxA7KW9GC2T3tQg3zHvjrnDPqfQUKTfzU3wbts8AsV6kH").unwrap();

    let keypair = utils::keypair_from_protobuf_encoding(&edge_dec_privkey);

    // Verify that we end up with the same PeerId
    assert_eq!(keypair.public().to_peer_id(), edge_peerid);
}
