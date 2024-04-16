use lazy_static::lazy_static;

lazy_static! {
    /// Size of the double echo command channel
    pub static ref COMMAND_CHANNEL_SIZE: usize =
        std::env::var("TOPOS_DOUBLE_ECHO_COMMAND_CHANNEL_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(20_480);
    /// Size of the channel between double echo and the task manager
    pub static ref BROADCAST_TASK_MANAGER_CHANNEL_SIZE: usize =
        std::env::var("TOPOS_BROADCAST_TASK_MANAGER_CHANNEL_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(20_480);
    /// Size of the channel to send protocol events from the double echo
    pub static ref PROTOCOL_CHANNEL_SIZE: usize =
        std::env::var("TOPOS_PROTOCOL_CHANNEL_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(20_480);
    /// Capacity alert threshold for the double echo command channel
    pub static ref COMMAND_CHANNEL_CAPACITY: usize = COMMAND_CHANNEL_SIZE
        .checked_mul(100)
        .map(|v| {
            let r: usize = v.checked_div(100).unwrap_or(*COMMAND_CHANNEL_SIZE);
            r
        })
        .unwrap_or(*COMMAND_CHANNEL_SIZE);
    ///
    pub static ref PENDING_LIMIT_PER_REQUEST_TO_STORAGE: usize =
        std::env::var("TOPOS_PENDING_LIMIT_PER_REQUEST_TO_STORAGE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000);
}
