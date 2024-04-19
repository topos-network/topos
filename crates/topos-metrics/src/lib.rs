use prometheus::{
    register_histogram_with_registry, register_int_counter_with_registry, Encoder, Histogram,
    IntCounter, Registry, TextEncoder,
};

use lazy_static::lazy_static;
use std::collections::hash_map::HashMap;

mod api;
pub mod channels;
mod double_echo;
mod p2p;
mod storage;

#[cfg(test)]
mod tests;

pub use api::*;
pub use double_echo::*;
pub use p2p::*;
pub use storage::*;

lazy_static! {
    pub static ref TOPOS_METRIC_REGISTRY: Registry = Registry::new_custom(
        Some("topos".to_string()),
        Some(HashMap::from([
            (
                "run_id".to_string(),
                std::env::var("TOPOS_RUN_ID")
                    .ok()
                    .unwrap_or("default".to_string())
            ),
            (
                "run_number".to_string(),
                std::env::var("TOPOS_RUN_NUMBER")
                    .ok()
                    .unwrap_or("default".to_string())
            )
        ]))
    )
    .unwrap();
    pub static ref CERTIFICATE_PROCESSING_TOTAL: IntCounter = register_int_counter_with_registry!(
        "certificate_processing_total",
        "Number of certificate received.",
        TOPOS_METRIC_REGISTRY
    )
    .unwrap();
    pub static ref CERTIFICATE_PROCESSING_FROM_GOSSIP_TOTAL: IntCounter =
        register_int_counter_with_registry!(
            "certificate_processing_from_gossip_total",
            "Number of certificate received from gossip.",
            TOPOS_METRIC_REGISTRY
        )
        .unwrap();
    pub static ref CERTIFICATE_PROCESSING_FROM_API_TOTAL: IntCounter =
        register_int_counter_with_registry!(
            "certificate_processing_from_api_total",
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
    pub static ref CERTIFICATE_DELIVERY_LATENCY: Histogram = register_histogram_with_registry!(
        "double_echo_delivery_latency",
        "Latency to delivery.",
        prometheus::linear_buckets(0.1, 0.01, 500).unwrap(),
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
    API_GRPC_CERTIFICATE_RECEIVED_TOTAL.reset();
    P2P_EVENT_STREAM_CAPACITY_TOTAL.reset();
    P2P_MESSAGE_RECEIVED_ON_GOSSIP_TOTAL.reset();
    P2P_MESSAGE_RECEIVED_ON_ECHO_TOTAL.reset();
    P2P_MESSAGE_RECEIVED_ON_READY_TOTAL.reset();
    P2P_MESSAGE_SENT_ON_GOSSIPSUB_TOTAL.reset();
    DOUBLE_ECHO_ACTIVE_TASKS_COUNT.set(0);
    DOUBLE_ECHO_COMMAND_CHANNEL_CAPACITY_TOTAL.reset();
    DOUBLE_ECHO_BUFFER_CAPACITY_TOTAL.reset();
    DOUBLE_ECHO_CURRENT_BUFFER_SIZE.set(0);
    DOUBLE_ECHO_BUFFERED_MESSAGE_COUNT.set(0);
    DOUBLE_ECHO_BROADCAST_CREATED_TOTAL.reset();
    DOUBLE_ECHO_BROADCAST_FINISHED_TOTAL.reset();
    CERTIFICATE_PROCESSING_TOTAL.reset();
    CERTIFICATE_PROCESSING_FROM_GOSSIP_TOTAL.reset();
    CERTIFICATE_PROCESSING_FROM_API_TOTAL.reset();
    CERTIFICATE_DELIVERED_TOTAL.reset();
    STORAGE_COMMAND_CHANNEL_CAPACITY_TOTAL.reset();
}
