use std::{collections::HashMap, net::UdpSocket, str::FromStr};

use crate::SubnetId;
use futures::{future::join_all, Stream, StreamExt};
use libp2p::{
    identity::{self, Keypair},
    Multiaddr, PeerId,
};
use std::future::IntoFuture;
use tokio::task::JoinHandle;
use tokio::{
    spawn,
    sync::{mpsc, oneshot},
};
use tonic::transport::{channel, Channel};
use topos_core::api::tce::v1::{
    api_service_client::ApiServiceClient, console_service_client::ConsoleServiceClient,
};
use topos_p2p::{Client, Event, Runtime};
use topos_tce_broadcast::{
    DoubleEchoCommand, ReliableBroadcastClient, ReliableBroadcastConfig, SamplerCommand,
};
use topos_tce_transport::{ReliableBroadcastParams, TceEvents};
use tracing::{info_span, Instrument, Span};

#[derive(Debug)]
pub struct TestAppContext {
    pub peer_id: PeerId, // P2P ID
    pub command_sampler: mpsc::Sender<SamplerCommand>,
    pub command_broadcast: mpsc::Sender<DoubleEchoCommand>,
    pub(crate) api_grpc_client: ApiServiceClient<Channel>, // GRPC Client for this peer (tce node)
    pub(crate) console_grpc_client: ConsoleServiceClient<Channel>, // Console TCE GRPC Client for this peer (tce node)
    pub runtime_join_handle: JoinHandle<Result<(), ()>>,
    pub app_join_handle: JoinHandle<()>,
    pub storage_join_handle: JoinHandle<Result<(), topos_tce_storage::errors::StorageError>>,
    pub gatekeeper_join_handle: JoinHandle<Result<(), topos_tce_gatekeeper::GatekeeperError>>,
    pub synchronizer_join_handle: JoinHandle<Result<(), topos_tce_synchronizer::SynchronizerError>>,
    pub connected_subnets: Option<Vec<SubnetId>>, // Particular subnet clients (topos nodes) connected to this tce node
}

pub type Seed = u8;
pub type Port = u16;
pub type PeerConfig = (Seed, Port, Keypair, Multiaddr);

pub async fn start_peer_pool<F>(
    peer_number: u8,
    correct_sample: usize,
    g: F,
) -> HashMap<PeerId, TestAppContext>
where
    F: Fn(usize, f32) -> usize,
{
    let mut clients = HashMap::new();
    let peers = build_peer_config_pool(peer_number);

    let mut await_peers = Vec::new();
    for (seed, port, keypair, addr) in &peers {
        let span = info_span!("Peer", peer = keypair.public().to_peer_id().to_string());

        let fut = async {
            let (tce_cli, tce_stream) = create_reliable_broadcast_client(
                create_reliable_broadcast_params(correct_sample, &g),
                keypair.public().to_peer_id().to_string(),
            );
            let peer_id = keypair.public().to_peer_id();
            let (command_sampler, command_broadcast) = tce_cli.get_command_channels();

            let (network_client, network_stream, runtime) =
                create_network_worker(*seed, *port, addr.clone(), &peers, 2)
                    .instrument(Span::current())
                    .await;

            let runtime = runtime
                .bootstrap()
                .instrument(Span::current())
                .await
                .expect("Unable to bootstrap tce network");

            let socket = UdpSocket::bind("0.0.0.0:0").expect("Can't find an available port");
            let addr = socket.local_addr().ok().unwrap();
            let api_port = addr.port();

            let peer_id_str = peer_id.to_base58();
            // launch data store
            let (_, (storage, storage_client, storage_stream)) =
                topos_test_sdk::storage::create_rocksdb(&peer_id_str, &[]).await;
            let storage_join_handle = spawn(storage.into_future());

            let (api_client, api_events) = topos_tce_api::Runtime::builder()
                .serve_addr(addr)
                .storage(storage_client.clone())
                .build_and_launch()
                .instrument(Span::current())
                .await;

            let (gatekeeper_client, gatekeeper_runtime) =
                topos_tce_gatekeeper::Gatekeeper::builder()
                    .local_peer_id(keypair.public().to_peer_id())
                    .await
                    .expect("Can't create the Gatekeeper");
            let gatekeeper_join_handle = spawn(gatekeeper_runtime.into_future());

            let (synchronizer_client, synchronizer_runtime, synchronizer_stream) =
                topos_tce_synchronizer::Synchronizer::builder()
                    .with_gatekeeper_client(gatekeeper_client.clone())
                    .with_network_client(network_client.clone())
                    .await
                    .expect("Can't create the Synchronizer");
            let synchronizer_join_handle = spawn(synchronizer_runtime.into_future());

            let app = topos_tce::AppContext::new(
                storage_client,
                tce_cli,
                network_client,
                api_client,
                gatekeeper_client,
                synchronizer_client,
            );

            let (_shutdown_sender, shutdown_receiver) = mpsc::channel::<oneshot::Sender<()>>(1);
            let runtime_join_handle = spawn(runtime.run().instrument(Span::current()));
            let app_join_handle = spawn(
                app.run(
                    network_stream,
                    tce_stream,
                    api_events,
                    storage_stream,
                    synchronizer_stream,
                    shutdown_receiver,
                )
                .instrument(Span::current()),
            );

            let api_endpoint = format!("http://127.0.0.1:{api_port}");

            let channel = channel::Endpoint::from_str(&api_endpoint)
                .unwrap()
                .connect_lazy();
            let api_grpc_client = ApiServiceClient::new(channel.clone());
            let console_grpc_client = ConsoleServiceClient::new(channel);
            let peer_id = keypair.public().to_peer_id();

            let client = TestAppContext {
                peer_id,
                command_sampler,
                command_broadcast,
                api_grpc_client,
                console_grpc_client,
                runtime_join_handle,
                app_join_handle,
                storage_join_handle,
                gatekeeper_join_handle,
                synchronizer_join_handle,
                connected_subnets: None,
            };
            (peer_id, client)
        }
        .instrument(span);

        await_peers.push(fut);
    }

    for (user_peer_id, client) in join_all(await_peers).await {
        clients.insert(user_peer_id, client);
    }

    clients
}

fn build_peer_config_pool(peer_number: u8) -> Vec<PeerConfig> {
    (1..=peer_number)
        .into_iter()
        .map(|id| {
            let (peer_id, port, addr) = local_peer(id);

            (id, port, peer_id, addr)
        })
        .collect()
}

fn local_peer(peer_index: u8) -> (Keypair, Port, Multiaddr) {
    let peer_id: Keypair = keypair_from_seed(peer_index);
    let socket = UdpSocket::bind("0.0.0.0:0").expect("Can't find an available port");
    let port = socket.local_addr().unwrap().port();
    let local_listen_addr: Multiaddr = format!(
        "/ip4/127.0.0.1/tcp/{}/p2p/{}",
        port,
        peer_id.public().to_peer_id()
    )
    .parse()
    .unwrap();

    (peer_id, port, local_listen_addr)
}

fn keypair_from_seed(seed: u8) -> Keypair {
    let mut bytes = [0u8; 32];
    bytes[0] = seed;
    let secret_key = identity::ed25519::SecretKey::from_bytes(&mut bytes)
        .expect("this returns `Err` only if the length is wrong; the length is correct; qed");
    identity::Keypair::Ed25519(secret_key.into())
}

async fn create_network_worker(
    seed: u8,
    _port: u16,
    addr: Multiaddr,
    peers: &[PeerConfig],
    minimum_cluster_size: usize,
) -> (Client, impl Stream<Item = Event> + Unpin + Send, Runtime) {
    let key = keypair_from_seed(seed);
    let _peer_id = key.public().to_peer_id();

    let known_peers = if seed == 1 {
        vec![]
    } else {
        peers
            .iter()
            .filter_map(|(current_seed, _, key, addr)| {
                if *current_seed == 1 {
                    Some((key.public().to_peer_id(), addr.clone().into()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    };

    topos_p2p::network::builder()
        .peer_key(key.clone())
        .known_peers(&known_peers)
        .exposed_addresses(addr.clone())
        .listen_addr(addr)
        .minimum_cluster_size(minimum_cluster_size)
        .build()
        .await
        .expect("Cannot create network")
}

fn create_reliable_broadcast_client(
    tce_params: ReliableBroadcastParams,
    peer_id: String,
) -> (
    ReliableBroadcastClient,
    impl Stream<Item = Result<TceEvents, ()>> + Unpin,
) {
    let config = ReliableBroadcastConfig { tce_params };

    ReliableBroadcastClient::new(config, peer_id)
}

fn create_reliable_broadcast_params<F>(correct_sample: usize, g: F) -> ReliableBroadcastParams
where
    F: Fn(usize, f32) -> usize,
{
    let mut params = ReliableBroadcastParams::default();
    params.ready_sample_size = correct_sample;
    params.echo_sample_size = correct_sample;
    params.delivery_sample_size = correct_sample;

    let e_ratio: f32 = 0.5;
    let r_ratio: f32 = 0.5;
    let d_ratio: f32 = 0.5;

    params.echo_threshold = g(params.echo_sample_size, e_ratio);
    params.ready_threshold = g(params.ready_sample_size, r_ratio);
    params.delivery_threshold = g(params.delivery_sample_size, d_ratio);

    params
}

#[allow(dead_code)]
pub struct TestNodeContext {
    pub(crate) peer_id: PeerId,
    pub(crate) peer_addr: Multiaddr,
    pub(crate) client: Client,
    stream: Box<dyn Stream<Item = topos_p2p::Event> + Unpin + Send>,
}

impl TestNodeContext {
    #[allow(dead_code)]
    pub(crate) async fn next_event(&mut self) -> Option<topos_p2p::Event> {
        self.stream.next().await
    }
}
