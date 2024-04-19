use prometheus::{
    register_int_counter_with_registry, register_int_gauge_with_registry, IntCounter, IntGauge,
};

use lazy_static::lazy_static;

use crate::TOPOS_METRIC_REGISTRY;

lazy_static! {
    pub static ref DOUBLE_ECHO_EVENT_CHANNEL: IntGauge = register_int_gauge_with_registry!(
        "double_echo_event_channel",
        "Gauge for the double echo event channel.",
        TOPOS_METRIC_REGISTRY
    )
    .unwrap();
    pub static ref DOUBLE_CLIENT_TO_DOUBLE_ECHO_CHANNEL: IntGauge =
        register_int_gauge_with_registry!(
            "double_client_to_double_echo_channel",
            "Gauge for the double echo command channel between client and double echo.",
            TOPOS_METRIC_REGISTRY
        )
        .unwrap();
    pub static ref DOUBLE_ECHO_TO_TASK_MANAGER_CHANNEL: IntGauge =
        register_int_gauge_with_registry!(
            "double_echo_to_task_manager_channel",
            "Gauge for the double echo command channel between double echo and task manager.",
            TOPOS_METRIC_REGISTRY
        )
        .unwrap();
    pub static ref DOUBLE_ECHO_ACTIVE_TASKS_COUNT: IntGauge = register_int_gauge_with_registry!(
        "double_echo_active_tasks_count",
        "Number of active tasks in the double echo.",
        TOPOS_METRIC_REGISTRY
    )
    .unwrap();
    pub static ref DOUBLE_ECHO_COMMAND_CHANNEL_CAPACITY_TOTAL: IntCounter =
        register_int_counter_with_registry!(
            "double_echo_command_channel_capacity_total",
            "Number of time the double echo command channel was at capacity.",
            TOPOS_METRIC_REGISTRY
        )
        .unwrap();
    pub static ref DOUBLE_ECHO_BUFFER_CAPACITY_TOTAL: IntCounter =
        register_int_counter_with_registry!(
            "double_echo_buffer_capacity_total",
            "Number of time the double echo buffer was at capacity.",
            TOPOS_METRIC_REGISTRY
        )
        .unwrap();
    pub static ref DOUBLE_ECHO_CURRENT_BUFFER_SIZE: IntGauge = register_int_gauge_with_registry!(
        "double_echo_current_buffer_size",
        "Current size of the double echo buffer.",
        TOPOS_METRIC_REGISTRY
    )
    .unwrap();
    pub static ref DOUBLE_ECHO_BUFFERED_MESSAGE_COUNT: IntGauge =
        register_int_gauge_with_registry!(
            "double_echo_buffered_message_count",
            "Number of message buffered in the double echo buffer.",
            TOPOS_METRIC_REGISTRY
        )
        .unwrap();
    pub static ref DOUBLE_ECHO_BROADCAST_CREATED_TOTAL: IntCounter =
        register_int_counter_with_registry!(
            "double_echo_broadcast_created_total",
            "Number of broadcast created.",
            TOPOS_METRIC_REGISTRY
        )
        .unwrap();
    pub static ref DOUBLE_ECHO_BROADCAST_FINISHED_TOTAL: IntCounter =
        register_int_counter_with_registry!(
            "double_echo_broadcast_finished_total",
            "Number of broadcast finished.",
            TOPOS_METRIC_REGISTRY
        )
        .unwrap();
}
