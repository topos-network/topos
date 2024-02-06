use std::{num::NonZeroUsize, time::Duration};

use futures::StreamExt;
use libp2p::{
    kad::{record::Key, Event, PutRecordOk, QueryResult, Record},
    swarm::SwarmEvent,
    Multiaddr,
};
use rstest::rstest;
use test_log::test;
use topos_test_sdk::tce::NodeConfig;

use crate::{config::DiscoveryConfig, event::ComposedEvent, wait_for_event};

#[rstest]
#[test(tokio::test)]
#[timeout(Duration::from_secs(5))]
async fn put_value_in_dht() {
    let peer_1 = NodeConfig::from_seed(1);
    let peer_2 = NodeConfig::from_seed(2);

    let (_client, _, join) = peer_1.bootstrap(&[], None).await.unwrap();

    let (_, _, mut runtime) = crate::network::builder()
        .peer_key(peer_2.keypair.clone())
        .known_peers(&[(peer_1.peer_id(), peer_1.addr.clone())])
        .public_addresses(vec![peer_2.addr.clone()])
        .listen_addresses(vec![peer_2.addr.clone()])
        .minimum_cluster_size(1)
        .discovery_config(
            DiscoveryConfig::default().with_replication_factor(NonZeroUsize::new(1).unwrap()),
        )
        .build()
        .await
        .expect("Unable to create p2p network");

    runtime.bootstrap().await.unwrap();
    let kad = &mut runtime.swarm.behaviour_mut().discovery;

    let input_key = Key::new(&runtime.local_peer_id.to_string());
    _ = kad
        .inner
        .put_record(
            Record::new(
                input_key.clone(),
                runtime
                    .public_addresses
                    .first()
                    .map(Multiaddr::to_vec)
                    .unwrap(),
            ),
            libp2p::kad::Quorum::One,
        )
        .unwrap();

    let mut swarm = runtime.swarm;

    wait_for_event!(
        swarm,
        matches: SwarmEvent::Behaviour(ComposedEvent::Kademlia(kademlia_event)) if matches!(&*kademlia_event, Event::OutboundQueryProgressed { result: QueryResult::PutRecord(Ok(PutRecordOk { key: input_key })), .. })
    );

    join.abort();
}
