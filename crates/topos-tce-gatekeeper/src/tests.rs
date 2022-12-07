use std::future::IntoFuture;

use rstest::{fixture, rstest};
use tokio::spawn;
use topos_p2p::PeerId;

use crate::{Gatekeeper, GatekeeperClient};

#[tokio::test]
async fn can_be_start_and_stop() -> Result<(), Box<dyn std::error::Error>> {
    let (client, server) = Gatekeeper::builder().await?;

    let handler = spawn(server.into_future());

    client.shutdown().await?;

    assert!(handler.is_finished());

    Ok(())
}

#[rstest]
#[tokio::test]
async fn can_push_a_peer_list(
    #[future] gatekeeper: GatekeeperClient,
    #[with(10)] peer_list: Vec<PeerId>,
) {
    let gatekeeper = gatekeeper.await;

    gatekeeper.push_peer_list(peer_list).await.unwrap();

    assert_eq!(10, gatekeeper.get_all_peers().await.unwrap().len());
}

#[fixture]
async fn gatekeeper() -> GatekeeperClient {
    let (client, server) = Gatekeeper::builder().await.unwrap();

    spawn(server.into_future());

    client
}

#[fixture]
fn peer_list(#[default(1)] number: usize) -> Vec<PeerId> {
    (0..number)
        .map(|i| {
            topos_p2p::utils::local_key_pair(Some(i as u8))
                .public()
                .to_peer_id()
        })
        .collect()
}
