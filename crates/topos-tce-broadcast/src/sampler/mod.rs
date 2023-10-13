use std::collections::HashSet;
use topos_core::types::ValidatorId;

/// Categorize what we expect from which peer for the broadcast
#[derive(PartialEq, Eq, Hash, Debug, Clone)]
pub enum SampleType {
    /// Listen Echo from this Sample
    EchoSubscription,
    /// Listen Ready from this Sample
    ReadySubscription,
    /// Send Echo to this Sample
    EchoSubscriber,
    /// Send Ready to this Sample
    ReadySubscriber,
}

/// Stateful network view with whom we broadcast the Certificate
/// The Echo and the Ready sets are initially equal to the whole network
#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct SubscriptionsView {
    /// Set of Peer from which we listen for ECHO messages
    pub echo: HashSet<ValidatorId>,
    /// Set of Peer from which we listen for READY messages
    pub ready: HashSet<ValidatorId>,
    /// Size of the network
    pub network_size: usize,
}

impl SubscriptionsView {
    pub fn is_some(&self) -> bool {
        !self.is_none()
    }

    pub fn is_none(&self) -> bool {
        self.echo.is_empty() && self.ready.is_empty()
    }
}
