use dockertest::{
    Composition, DockerTest, Image, LogAction, LogOptions, LogPolicy, LogSource, PullPolicy, Source,
};
use fs_extra::dir::{copy, create_all, CopyOptions};
use rstest::*;
use secp256k1::SecretKey;
use serial_test::serial;
use std::env;
use std::path::PathBuf;
use std::str::FromStr;
use tokio::sync::oneshot;
use topos_core::uci::{Certificate, CertificateId, SubnetId};
use web3::contract::tokens::Tokenize;
use web3::ethabi::Token;
use web3::transports::Http;
use web3::types::{BlockNumber, Log};

mod common;
use common::subnet_test_data::{generate_test_keystore_file, TEST_KEYSTORE_FILE_PASSWORD};
use topos_sequencer_subnet_runtime_proxy::{RuntimeProxyConfig, RuntimeProxyWorker};

const SUBNET_TCC_JSON_DEFINITION: &'static str = "ToposCoreContract.json";
const SUBNET_TOKEN_DEPLOYER_JSON_DEFINITION: &'static str = "TokenDeployer.json";
const SUBNET_CHAIN_ID: u64 = 100;
const SUBNET_RPC_PORT: u32 = 8545;
const TEST_SECRET_ETHEREUM_KEY: &'static str =
    "5fb92d6e98884f76de468fa3f6278f8807c48bebc13595d45af5bdc4da702133";
const POLYGON_EDGE_CONTAINER: &'static str = "ghcr.io/toposware/polygon-edge";
const POLYGON_EDGE_CONTAINER_TAG: &str = "develop";
const SUBNET_STARTUP_DELAY: u64 = 5; // seconds left for subnet startup
const TOPOS_SMART_CONTRACTS_BUILD_PATH_VAR: &str = "TOPOS_SMART_CONTRACTS_BUILD_PATH";

const SOURCE_SUBNET_ID: SubnetId = [1u8; 32];
const SOURCE_SUBNET_ID_2: SubnetId = [2u8; 32];
const PREV_CERTIFICATE_ID: CertificateId = CertificateId::from_array([4u8; 32]);
const CERTIFICATE_ID: CertificateId = CertificateId::from_array([5u8; 32]);
const CERTIFICATE_ID_2: CertificateId = CertificateId::from_array([6u8; 32]);

async fn deploy_contract<T, U>(
    contract_file_path: &str,
    web3_client: &mut web3::Web3<Http>,
    eth_private_key: &SecretKey,
    params: Option<(T, U)>,
) -> Result<web3::contract::Contract<Http>, Box<dyn std::error::Error>>
where
    (T, U): Tokenize,
{
    println!("Parsing  contract file {}", contract_file_path);
    let contract_file = std::fs::File::open(contract_file_path).unwrap();
    let contract_json: serde_json::Value =
        serde_json::from_reader(contract_file).expect("error while reading or parsing");
    let contract_abi = contract_json.get("abi").unwrap().to_string();
    let contract_bytecode = contract_json
        .get("bytecode")
        .unwrap()
        .to_string()
        .replace('"', "");
    // Deploy contract, check result
    let deployment = web3::contract::Contract::deploy(web3_client.eth(), contract_abi.as_bytes())?
        .confirmations(1)
        .options(web3::contract::Options::with(|opt| {
            opt.gas = Some(3_000_000.into());
        }));

    let deployment_result = if params.is_none() {
        // Contract without contstructor
        deployment
            .sign_with_key_and_execute(
                contract_bytecode,
                (),
                eth_private_key,
                Some(SUBNET_CHAIN_ID),
            )
            .await
    } else {
        // Contract with 2 arguments
        deployment
            .sign_with_key_and_execute(
                contract_bytecode,
                params.unwrap(),
                eth_private_key,
                Some(SUBNET_CHAIN_ID),
            )
            .await
    };

    match deployment_result {
        Ok(contract) => {
            println!(
                "Contract {} deployment ok, new contract address: {}",
                contract_file_path,
                contract.address()
            );
            Ok(contract)
        }
        Err(e) => {
            println!("Contract {} deployment error {}", contract_file_path, e);
            Err(Box::<dyn std::error::Error>::from(e))
        }
    }
}

async fn deploy_contracts(
    web3_client: &mut web3::Web3<Http>,
) -> Result<
    (
        web3::contract::Contract<Http>,
        web3::contract::Contract<Http>,
    ),
    Box<dyn std::error::Error>,
> {
    println!("Deploying subnet smart contract...");
    let eth_private_key = secp256k1::SecretKey::from_str(TEST_SECRET_ETHEREUM_KEY)?;

    println!("Getting Token deployer definition...");
    // Deploy subnet smart contract (currently topos core contract)
    let token_deployer_contract_file_path = match std::env::var(
        TOPOS_SMART_CONTRACTS_BUILD_PATH_VAR,
    ) {
        Ok(path) => path + "/" + SUBNET_TOKEN_DEPLOYER_JSON_DEFINITION,
        Err(_e) => {
            println!("Error reading contract build path from `TOPOS_SMART_CONTRACTS_BUILD_PATH` environment variable, using current folder {}",
                     std::env::current_dir().unwrap().display());
            String::from(SUBNET_TOKEN_DEPLOYER_JSON_DEFINITION)
        }
    };

    let token_deployer_contract = deploy_contract(
        &token_deployer_contract_file_path,
        web3_client,
        &eth_private_key,
        Option::<(Token, Token)>::None,
    )
    .await?;

    println!("Getting Topos Core Contract definition...");
    // Deploy subnet smart contract (currently topos core contract)
    let tcc_contract_file_path = match std::env::var(TOPOS_SMART_CONTRACTS_BUILD_PATH_VAR) {
        Ok(path) => path + "/" + SUBNET_TCC_JSON_DEFINITION,
        Err(_e) => {
            println!("Error reading contract build path from `TOPOS_SMART_CONTRACTS_BUILD_PATH` environment variable, using current folder {}",
                     std::env::current_dir().unwrap().display());
            String::from(SUBNET_TCC_JSON_DEFINITION)
        }
    };
    let topos_core_contract = deploy_contract(
        &tcc_contract_file_path,
        web3_client,
        &eth_private_key,
        Some((
            token_deployer_contract.address(),
            SOURCE_SUBNET_ID.to_owned(),
        )),
    )
    .await?;
    Ok((topos_core_contract, token_deployer_contract))
}

fn spawn_subnet_node(
    stop_subnet_receiver: tokio::sync::oneshot::Receiver<()>,
    subnet_ready_sender: tokio::sync::oneshot::Sender<()>,
    port: u32,
) -> Result<tokio::task::JoinHandle<()>, Box<dyn std::error::Error>> {
    let handle = tokio::task::spawn_blocking(move || {
        let source = Source::DockerHub;
        let mut temp_dir = PathBuf::from_str(env!("CARGO_TARGET_TMPDIR"))
            .expect("Unable to read CARGO_TARGET_TMPDIR");
        temp_dir.push(format!(
            "./topos-sequencer/data_{}_{}",
            port,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("valid system time duration")
                .as_millis()
                .to_string()
        ));
        let mut test_chain_dir = temp_dir.clone();
        let mut genesis_dir = temp_dir.clone();
        test_chain_dir.push("./test-chain-1");
        genesis_dir.push("./genesis");

        println!("genesis_dir {:?}", genesis_dir);
        let img = Image::with_repository(POLYGON_EDGE_CONTAINER)
            .source(Source::Local)
            .tag(POLYGON_EDGE_CONTAINER_TAG)
            .pull_policy(PullPolicy::IfNotPresent);
        let mut polygon_edge_node = Composition::with_image(img);
        let current_dir: String = std::env::current_dir()
            .expect("current_dir")
            .to_str()
            .unwrap()
            .to_string();

        // There is no option in dockertest to change running user, so all blockchain data files
        // will be created with root privileges. So we copy keys and network config to the temp folder
        // that should be removed manually after the test
        let copy_options = CopyOptions {
            overwrite: true,
            copy_inside: true,
            ..Default::default()
        };

        if let Err(err) = create_all(&temp_dir, false) {
            eprintln!("Unable to create temporary test data directory {err}");
        };
        if let Err(err) = copy(
            current_dir.clone() + "/tests/artifacts/test-chain-1",
            &test_chain_dir,
            &copy_options,
        ) {
            eprintln!("Unable to copy test chain directory {err}");
        };
        if let Err(err) = copy(
            current_dir.clone() + "/tests/artifacts/genesis",
            &genesis_dir,
            &copy_options,
        ) {
            eprintln!("Unable to copy genesis directory {err}");
        };

        // Define options
        polygon_edge_node
            .bind_mount(
                test_chain_dir.into_os_string().to_string_lossy(),
                "/data/test-chain-1",
            )
            .bind_mount(genesis_dir.into_os_string().to_string_lossy(), "/genesis")
            .port_map(SUBNET_RPC_PORT, port);

        let mut polygon_edge_node_docker = DockerTest::new().with_default_source(source);
        // Setup command for polygon edge binary
        let cmd: Vec<String> = vec![
            "server".to_string(),
            "--data-dir".to_string(),
            "/data/test-chain-1".to_string(),
            "--chain".to_string(),
            "/genesis/genesis.json".to_string(),
        ];

        polygon_edge_node_docker.add_composition(
            polygon_edge_node
                .with_log_options(Some(LogOptions {
                    action: LogAction::Forward,
                    policy: LogPolicy::OnError,
                    source: LogSource::StdErr,
                }))
                .with_cmd(cmd),
        );
        polygon_edge_node_docker.run(|ops| async move {
            let container = ops.handle(POLYGON_EDGE_CONTAINER);
            println!(
                "Running container with id: {} name: {} ...",
                container.id(),
                container.name(),
            );
            // TODO: use polling of network block number or some other means to learn when subnet node has started
            tokio::time::sleep(tokio::time::Duration::from_secs(SUBNET_STARTUP_DELAY)).await;
            subnet_ready_sender
                .send(())
                .expect("subnet ready channel available");

            println!("Waiting for signal to close...");
            stop_subnet_receiver.await.unwrap();
            println!("Container id={} execution finished", container.id());
        })
    });

    Ok(handle)
}

async fn read_logs_for_address(
    contract_address: &str,
    web3_client: &web3::Web3<Http>,
    event_signature: &str,
) -> Result<Vec<Log>, Box<dyn std::error::Error>> {
    let event_topic: [u8; 32] = tiny_keccak::keccak256(event_signature.as_bytes());
    println!(
        "Looking for event signature {} and from address {}",
        hex::encode(&event_topic),
        contract_address
    );
    let filter = web3::types::FilterBuilder::default()
        .from_block(BlockNumber::Number(web3::types::U64::from(0)))
        .to_block(BlockNumber::Number(web3::types::U64::from(1000)))
        .address(vec![
            web3::ethabi::Address::from_str(contract_address).unwrap()
        ])
        .topics(
            Some(vec![web3::types::H256::from(event_topic)]),
            None,
            None,
            None,
        )
        .build();
    Ok(web3_client.eth().logs(filter).await?)
}

#[allow(dead_code)]
struct Context {
    pub subnet_contract: web3::contract::Contract<Http>,
    pub erc20_contract: web3::contract::Contract<Http>,
    pub subnet_node_handle: Option<tokio::task::JoinHandle<()>>,
    pub subnet_stop_sender: Option<tokio::sync::oneshot::Sender<()>>,
    pub web3_client: web3::Web3<Http>,
    pub port: u32,
}

impl Context {
    pub async fn shutdown(mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Send shutdown message to subnet node
        self.subnet_stop_sender
            .take()
            .expect("valid subnet stop channel")
            .send(())
            .expect("invalid subnet stop channel");

        // Wait for the subnet node to close
        self.subnet_node_handle
            .take()
            .expect("Valid subnet node handle")
            .await?;
        Ok(())
    }

    pub fn jsonrpc(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    pub fn jsonrpc_ws(&self) -> String {
        format!("ws://127.0.0.1:{}/ws", self.port)
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        // TODO: cleanup if necessary
    }
}

#[fixture]
async fn context_running_subnet_node(#[default(8545)] port: u32) -> Context {
    let (subnet_stop_sender, subnet_stop_receiver) = oneshot::channel::<()>();
    let (subnet_ready_sender, subnet_ready_receiver) = oneshot::channel::<()>();
    println!("Starting subnet node...");

    let subnet_node_handle = match spawn_subnet_node(
        subnet_stop_receiver,
        subnet_ready_sender,
        port,
    ) {
        Ok(subnet_node_handle) => subnet_node_handle,
        Err(e) => {
            println!("Failed to start substrate subnet node as part of test context, error details {}, panicking!!!", e);
            panic!("Unable to start substrate subnet node");
        }
    };

    let json_rpc_endpoint = format!("http://127.0.0.1:{}", port);

    subnet_ready_receiver
        .await
        .expect("subnet ready channel error");
    println!("Subnet node started...");
    let mut i = 0;
    let http: Http = loop {
        i += 1;
        break match Http::new(&json_rpc_endpoint) {
            Ok(http) => {
                println!("Connected to subnet node...");
                Some(http)
            }
            Err(e) => {
                if i < 10 {
                    eprintln!("Unable to connect to subnet node: {e}");
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    continue;
                }
                None
            }
        };
    }
    .expect("Unable to connect to subnet node");

    let mut web3_client = web3::Web3::new(http);
    // Wait for subnet node to start
    // Perform subnet smart contract deployment
    let (subnet_contract, erc20_contract) = match deploy_contracts(&mut web3_client).await.map_err(
        |e| {
            println!("Failed to deploy subnet contract: {}", e);
            e
        },
    ) {
        Ok(contract) => contract,
        Err(e) => {
            println!("Failed to deploy subnet contract as part of the test context, error details {}, panicking!!!", e);
            panic!("Unable to deploy subnet contract");
        }
    };
    // Context with subnet container working in the background and deployed contract ready
    Context {
        subnet_contract,
        erc20_contract,
        subnet_node_handle: Some(subnet_node_handle),
        subnet_stop_sender: Some(subnet_stop_sender),
        web3_client,
        port,
    }
}

/// Test to start subnet and deploy subnet smart contract
#[rstest]
#[tokio::test]
#[serial]
async fn test_subnet_node_contract_deployment(
    #[with(8544)]
    #[future]
    context_running_subnet_node: Context,
) -> Result<(), Box<dyn std::error::Error>> {
    let context = context_running_subnet_node.await;
    println!("Subnet running in the background with deployed contract");
    context.shutdown().await?;
    println!("Subnet node test finished");
    Ok(())
}

/// Test subnet client RPC connection to subnet
#[rstest]
#[tokio::test]
#[serial]
async fn test_subnet_node_get_block_info(
    #[with(8545)]
    #[future]
    context_running_subnet_node: Context,
) -> Result<(), Box<dyn std::error::Error>> {
    //Context with subnet
    let context = context_running_subnet_node.await;
    let eth_private_key = hex::decode(TEST_SECRET_ETHEREUM_KEY)?;
    let _eth_address =
        topos_sequencer_subnet_client::subnet_contract::derive_eth_address(&eth_private_key)?;
    match topos_sequencer_subnet_client::SubnetClientListener::new(
        &context.jsonrpc_ws(),
        &("0x".to_string() + &hex::encode(context.subnet_contract.address())),
    )
    .await
    {
        Ok(mut subnet_client) => {
            match subnet_client
                .get_next_finalized_block(
                    &("0x".to_string() + &hex::encode(context.subnet_contract.address())),
                )
                .await
            {
                Ok(block_info) => {
                    println!(
                        "Block info successfully retrieved for block {}",
                        block_info.number
                    );
                    assert!(block_info.number > 0 && block_info.number < 100);
                }
                Err(e) => {
                    eprintln!("Error getting next finalized block {e}");
                    panic!("Unable to get block info");
                }
            }
        }
        Err(e) => {
            eprintln!("Unable to get block info, error {}", e);
            panic!("Unable to get block info");
        }
    }
    context.shutdown().await?;
    println!("Subnet node test finished");
    Ok(())
}

/// Test runtime initialization
#[rstest]
#[tokio::test]
#[serial]
async fn test_create_runtime() -> Result<(), Box<dyn std::error::Error>> {
    let keystore_file_path = generate_test_keystore_file()?;
    println!("Creating runtime proxy...");
    let runtime_proxy_worker = RuntimeProxyWorker::new(RuntimeProxyConfig {
        subnet_id: SOURCE_SUBNET_ID,
        endpoint: format!("localhost:{}", SUBNET_RPC_PORT),
        subnet_contract_address: "0x0000000000000000000000000000000000000000".to_string(),
        keystore_file: keystore_file_path,
        keystore_password: TEST_KEYSTORE_FILE_PASSWORD.to_string(),
    })?;
    let runtime_proxy =
        topos_sequencer_subnet_runtime_proxy::testing::get_runtime(&runtime_proxy_worker);
    let runtime_proxy = runtime_proxy.lock();
    println!("New runtime proxy created:{:?}", &runtime_proxy);
    Ok(())
}

/// Test push certificate to subnet smart contract
#[rstest]
#[tokio::test]
#[serial]
async fn test_subnet_certificate_push_call(
    #[with(8546)]
    #[future]
    context_running_subnet_node: Context,
) -> Result<(), Box<dyn std::error::Error>> {
    let context = context_running_subnet_node.await;
    let keystore_file_path = generate_test_keystore_file()?;
    let subnet_smart_contract_address =
        "0x".to_string() + &hex::encode(context.subnet_contract.address());
    let runtime_proxy_worker = RuntimeProxyWorker::new(RuntimeProxyConfig {
        subnet_id: SOURCE_SUBNET_ID,
        endpoint: context.jsonrpc(),
        subnet_contract_address: subnet_smart_contract_address.clone(),
        keystore_file: keystore_file_path,
        keystore_password: TEST_KEYSTORE_FILE_PASSWORD.to_string(),
    })?;

    // TODO: Adjust this mock certificate when ToposCoreContract gets stable enough
    let mock_cert = Certificate {
        source_subnet_id: SOURCE_SUBNET_ID,
        id: CERTIFICATE_ID,
        prev_id: PREV_CERTIFICATE_ID,
        target_subnets: vec![SOURCE_SUBNET_ID],
    };
    println!("Sending mock certificate to subnet smart contract...");
    if let Err(e) = runtime_proxy_worker
        .eval(topos_sequencer_types::RuntimeProxyCommand::OnNewDeliveredTxns(mock_cert))
    {
        eprintln!("Failed to send OnNewDeliveredTxns command: {}", e);
        return Err(Box::from(e));
    }
    println!("Waiting for 10 seconds...");
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    println!("Checking logs for event...");
    let logs = read_logs_for_address(
        &subnet_smart_contract_address,
        &context.web3_client,
        "CertStored(bytes32)",
    )
    .await?;
    println!("Acquired logs from subnet smart contract: {:#?}", logs);
    assert_eq!(logs.len(), 1);
    assert_eq!(
        hex::encode(logs[0].address),
        subnet_smart_contract_address[2..]
    );
    println!("Shutting down context...");
    context.shutdown().await?;
    Ok(())
}

/// Test get last certificate id from subnet smart contract
#[rstest]
#[tokio::test]
#[serial]
async fn test_subnet_certificate_get_last_pushed_call(
    #[with(8546)]
    #[future]
    context_running_subnet_node: Context,
) -> Result<(), Box<dyn std::error::Error>> {
    let context = context_running_subnet_node.await;
    let subnet_smart_contract_address =
        "0x".to_string() + &hex::encode(context.subnet_contract.address());
    let subnet_jsonrpc_endpoint = "http://".to_string() + &context.jsonrpc();

    // Get last pushed certificate when contract is empty
    let subnet_client = topos_sequencer_subnet_client::SubnetClient::new(
        &subnet_jsonrpc_endpoint,
        hex::decode(TEST_SECRET_ETHEREUM_KEY).unwrap(),
        &subnet_smart_contract_address,
    )
    .await
    .expect("Valid subnet client");
    let (cert_id, cert_position) = match subnet_client.get_latest_pushed_cert().await {
        Ok(result) => result,
        Err(e) => {
            eprintln!("Unable to get latest certificate id and position, error details: {e}");
            panic!()
        }
    };

    assert_eq!((cert_id, cert_position), (Default::default(), 0));

    //TODO: Adjust this mock certificate when ToposCoreContract gets stable enough
    let test_certificates = vec![
        Certificate {
            source_subnet_id: SOURCE_SUBNET_ID_2,
            id: CERTIFICATE_ID,
            prev_id: PREV_CERTIFICATE_ID,
            target_subnets: vec![SOURCE_SUBNET_ID],
        },
        Certificate {
            source_subnet_id: SOURCE_SUBNET_ID_2,
            id: CERTIFICATE_ID_2,
            prev_id: CERTIFICATE_ID,
            target_subnets: vec![SOURCE_SUBNET_ID],
        },
    ];

    for test_cert in &test_certificates {
        println!("Pushing certificate id={:?}", test_cert.id);
        match subnet_client.push_certificate(&test_cert).await {
            Ok(_) => {
                println!("Certificate id={:?} pushed", test_cert.id);
            }
            Err(e) => {
                eprintln!("Unable to push certificate, error details: {e}");
                panic!()
            }
        }
    }

    println!("Getting latest cert id and position");
    let (final_cert_id, final_cert_position) = match subnet_client.get_latest_pushed_cert().await {
        Ok(result) => result,
        Err(e) => {
            eprintln!("Unable to get latest certificate id and position, error details: {e}");
            panic!()
        }
    };
    assert_eq!(
        (final_cert_id, final_cert_position),
        (test_certificates.last().unwrap().id, 1)
    );

    println!("Shutting down context...");
    context.shutdown().await?;
    Ok(())
}
