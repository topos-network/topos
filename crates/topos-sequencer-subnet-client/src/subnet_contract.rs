use crate::{Error, SubnetEvent};
use ethers::abi::ethabi::ethereum_types::{H160, U64};
use ethers::contract::ContractError;
use ethers::signers::LocalWallet;
use ethers::{
    prelude::abigen,
    providers::{Middleware, Provider, Ws},
    signers::Signer,
};
use std::sync::Arc;
use tracing::info;

abigen!(
    IToposCore,
    "npm:@topos-protocol/topos-smart-contracts@3.4.0/artifacts/contracts/interfaces/IToposCore.\
     sol/IToposCore.json"
);

pub(crate) fn create_topos_core_contract_from_json<T: Middleware>(
    contract_address: &str,
    client: Arc<T>,
) -> Result<IToposCore<T>, Error> {
    let address: ethers::types::Address =
        contract_address.parse().map_err(Error::HexDecodingError)?;
    let contract = IToposCore::new(address, client);
    Ok(contract)
}

pub(crate) async fn get_block_events(
    contract: &IToposCore<Provider<Ws>>,
    block_number: U64,
) -> Result<Vec<crate::SubnetEvent>, Error> {
    // FIXME: There is some ethers issue when parsing events
    // from genesis block so skip it - we certainly don't expect any valid event here
    if block_number.as_u64() == 0 {
        return Ok(Vec::new());
    }

    // Parse only event from this particular block
    let events = contract
        .events()
        .from_block(block_number)
        .to_block(block_number);
    let topos_core_events = events.query_with_meta().await.map_err(|e| {
        match e {
            ContractError::DecodingError(e) => {
                // FIXME: events have decoding error in the blocks before contract is deployed
                Error::EventDecodingError(e.to_string())
            }
            _ => Error::ContractError(e.to_string()),
        }
    })?;

    let mut result = Vec::new();
    for event in topos_core_events {
        if let (IToposCoreEvents::CrossSubnetMessageSentFilter(f), meta) = event {
            info!(
                "Received CrossSubnetMessageSentFilter event: {f:?}, meta {:?}",
                meta
            );
            result.push(SubnetEvent::CrossSubnetMessageSent {
                target_subnet_id: f.target_subnet_id.into(),
                source_subnet_id: f.source_subnet_id.into(),
                nonce: f.nonce.as_u64(),
            })
        } else {
            // Ignored other events until we need them
        }
    }

    Ok(result)
}

pub fn derive_eth_address(secret_key: &[u8]) -> Result<H160, crate::Error> {
    let signer = hex::encode(secret_key)
        .parse::<LocalWallet>()
        .map_err(|e| Error::InvalidKey(e.to_string()))?;
    Ok(signer.address())
}
