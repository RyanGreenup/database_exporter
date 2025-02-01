use connectorx::prelude::*;
use duckdb::Connection;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use crate::sql_server::SQLServer;

struct DatabaseEngine {
    pub sql_server: SQLServer,
    pub postgres: SQLEngineConfig,
}




#[derive(Debug, Serialize, Deserialize)]
pub struct SQLEngineConfig {
    pub username: String,
    pub password: String,
    pub database: String,
    pub host: String,
    pub port: String,
    pub duckdb_conn: Connection
}


#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    #[serde(rename = "SQL Server")]
    pub sql_server: SQLEngineConfig,
    pub postgres: SQLEngineConfig,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self, String> {
        if !path.exists() {
            let default_config = Config {
                sql_server: SQLEngineConfig {
                    username: "".to_string(),
                    password: "".to_string(),
                    database: "".to_string(),
                    host: "".to_string(),
                    port: "".to_string(),
                },
                postgres: SQLEngineConfig {
                    username: "".to_string(),
                    password: "".to_string(),
                    database: "".to_string(),
                    host: "".to_string(),
                    port: "".to_string(),
                },
            };

            let toml = toml::to_string(&default_config).map_err(|e| e.to_string())?;
            fs::write(path, toml).map_err(|e| e.to_string())?;

            return Err(format!(
                "Config file created at {}. Please fill it out and try again.",
                path.display()
            ));
        }

        let contents = fs::read_to_string(path).map_err(|e| e.to_string())?;
        toml::from_str(&contents).map_err(|e| e.to_string())
    }
}
