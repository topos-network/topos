use prometheus::{
    self, register_int_counter_with_registry, Encoder, IntCounter, Registry, TextEncoder,
};

use lazy_static::lazy_static;

mod double_echo;
mod p2p;
mod storage;

pub use double_echo::*;
pub use p2p::*;
pub use storage::*;

lazy_static! {
    pub static ref TOPOS_METRIC_REGISTRY: Registry =
        Registry::new_custom(Some("topos".to_string()), None).unwrap();
    pub static ref CERTIFICATE_RECEIVED_TOTAL: IntCounter = register_int_counter_with_registry!(
        "certificate_received_total",
        "Number of certificate received.",
        TOPOS_METRIC_REGISTRY
    )
    .unwrap();
    pub static ref CERTIFICATE_RECEIVED_FROM_GOSSIP_TOTAL: IntCounter =
        register_int_counter_with_registry!(
            "certificate_received_from_gossip_total",
            "Number of certificate received from gossip.",
            TOPOS_METRIC_REGISTRY
        )
        .unwrap();
    pub static ref CERTIFICATE_RECEIVED_FROM_API_TOTAL: IntCounter =
        register_int_counter_with_registry!(
            "certificate_received_from_api_total",
            "Number of certificate received from api.",
            TOPOS_METRIC_REGISTRY
        )
        .unwrap();
    pub static ref CERTIFICATE_DELIVERED_TOTAL: IntCounter = register_int_counter_with_registry!(
        "certificate_delivered_total",
        "Number of certificate delivered.",
        TOPOS_METRIC_REGISTRY
    )
    .unwrap();
}

pub fn gather_metrics() -> String {
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();

    // Gather the metrics.
    let metric_families = prometheus::gather();
    // Encode them to send.
    encoder.encode(&metric_families, &mut buffer).unwrap();

    let topos_metrics = TOPOS_METRIC_REGISTRY.gather();
    encoder.encode(&topos_metrics, &mut buffer).unwrap();

    String::from_utf8(buffer.clone()).unwrap()
}

pub fn init_metrics() {
    P2P_EVENT_STREAM_CAPACITY_TOTAL.reset();
    P2P_MESSAGE_RECEIVED_ON_GOSSIP_TOTAL.reset();
    P2P_MESSAGE_RECEIVED_ON_ECHO_TOTAL.reset();
    P2P_MESSAGE_RECEIVED_ON_READY_TOTAL.reset();
    P2P_MESSAGE_SENT_ON_GOSSIPSUB_TOTAL.reset();
    DOUBLE_ECHO_COMMAND_CHANNEL_CAPACITY_TOTAL.reset();
    DOUBLE_ECHO_BUFFER_CAPACITY_TOTAL.reset();
    DOUBLE_ECHO_CURRENT_BUFFER_SIZE.set(0);
    DOUBLE_ECHO_BUFFERED_MESSAGE_COUNT.set(0);
    CERTIFICATE_RECEIVED_TOTAL.reset();
    CERTIFICATE_RECEIVED_FROM_GOSSIP_TOTAL.reset();
    CERTIFICATE_RECEIVED_FROM_API_TOTAL.reset();
    CERTIFICATE_DELIVERED_TOTAL.reset();
    STORAGE_COMMAND_CHANNEL_CAPACITY_TOTAL.reset();
}
