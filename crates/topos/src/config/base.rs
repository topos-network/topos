use std::path::Path;

use figment::{
    providers::{Format, Serialized, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};

use crate::components::node::commands::Init;
use crate::config::Config;

#[derive(Serialize, Deserialize, Debug)]
pub struct BaseConfig {
    #[serde(default = "default_name")]
    pub name: String,

    #[serde(default = "default_role")]
    pub role: String,

    #[serde(default = "default_subnet")]
    pub subnet: String,
}

fn default_name() -> String {
    "default".to_string()
}

fn default_role() -> String {
    "validator".to_string()
}

fn default_subnet() -> String {
    "topos".to_string()
}

impl Config for BaseConfig {
    type Command = Init;

    type Output = Self;

    fn load_from_file(figment: Figment, home: &Path) -> Figment {
        let home = home.join("config.toml");

        let base = Figment::new()
            .merge(Toml::file(home).nested())
            .select("base");

        figment.merge(base)
    }

    fn load_context(figment: Figment) -> Result<Self::Output, figment::Error> {
        figment.extract()
    }

    fn profile(&self) -> String {
        "base".to_string()
    }
}
