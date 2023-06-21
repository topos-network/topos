use async_graphql::{InputObject, SimpleObject};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

use super::errors::GraphQLServerError;

#[derive(Clone, Debug, Default, Serialize, Deserialize, SimpleObject, InputObject)]
#[graphql(input_name = "SubnetIdInput")]
pub struct SubnetId {
    pub value: String,
}

impl TryFrom<&SubnetId> for topos_uci::SubnetId {
    type Error = GraphQLServerError;

    fn try_from(value: &SubnetId) -> Result<Self, Self::Error> {
        Self::from_str(value.value.as_str()).map_err(|e| {
            tracing::error!("Failed to convert SubnetId from GraphQL input {e:?}");
            GraphQLServerError::ParseDataConnector
        })
    }
}
