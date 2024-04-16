mod graphql;
mod grpc;
mod metrics;
mod runtime;
mod stream;

#[cfg(test)]
mod tests;

pub(crate) mod constants {
    /// Constant size of every channel in the crate
    pub(crate) const CHANNEL_SIZE: usize = 10_000;

    /// Constant size of every transient stream channel in the crate
    pub(crate) const TRANSIENT_STREAM_CHANNEL_SIZE: usize = 4_096;
}
pub use runtime::{
    error::RuntimeError, Runtime, RuntimeClient, RuntimeCommand, RuntimeContext, RuntimeEvent,
};
