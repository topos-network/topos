use tokio::sync::{mpsc, oneshot};
use topos_core::uci::SubnetId;
use topos_p2p::PeerId;

use crate::{
    GatekeeperCommand, GatekeeperError, GetAllPeers, GetAllSubnets, GetRandomPeers, PushPeerList,
};

#[derive(Clone)]
pub struct GatekeeperClient {
    #[allow(dead_code)]
    pub(crate) shutdown_channel: mpsc::Sender<oneshot::Sender<()>>,
    #[allow(dead_code)]
    pub(crate) commands: mpsc::Sender<GatekeeperCommand>,
}

impl GatekeeperClient {
    pub async fn shutdown(&self) -> Result<(), GatekeeperError> {
        let (sender, receiver) = oneshot::channel();
        self.shutdown_channel
            .send(sender)
            .await
            .map_err(GatekeeperError::ShutdownCommunication)?;

        Ok(receiver.await?)
    }

    pub async fn push_peer_list(
        &self,
        peer_list: Vec<PeerId>,
    ) -> Result<Vec<PeerId>, GatekeeperError> {
        PushPeerList { peer_list }.send_to(&self.commands).await
    }

    pub async fn get_all_peers(&self) -> Result<Vec<PeerId>, GatekeeperError> {
        GetAllPeers.send_to(&self.commands).await
    }

    pub async fn get_random_peers(&self, number: usize) -> Result<Vec<PeerId>, GatekeeperError> {
        GetRandomPeers { number }.send_to(&self.commands).await
    }

    pub async fn get_all_subnets(&self) -> Result<Vec<SubnetId>, GatekeeperError> {
        GetAllSubnets.send_to(&self.commands).await
    }
}
