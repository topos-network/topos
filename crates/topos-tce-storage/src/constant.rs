use lazy_static::lazy_static;

lazy_static! {
    pub static ref COMMAND_CHANNEL_SIZE: usize =
        std::env::var("TOPOS_STORAGE_COMMAND_CHANNEL_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(2048);
    pub static ref COMMAND_CHANNEL_CAPACITY: usize = COMMAND_CHANNEL_SIZE
        .checked_mul(10)
        .map(|v| {
            let r: usize = v.checked_div(100).unwrap_or(*COMMAND_CHANNEL_SIZE);
            r
        })
        .unwrap_or(*COMMAND_CHANNEL_SIZE);
}
