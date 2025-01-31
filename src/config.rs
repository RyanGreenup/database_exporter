use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Serialize, Deserialize)]
pub struct SqlServerConfig {
    pub username: String,
    pub password: String,
    pub database: String,
    pub host: String,
    pub port: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PostgresConfig {
    pub username: String,
    pub password: String,
    pub database: String,
    pub host: String,
    pub port: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    #[serde(rename = "SQL Server")]
    pub sql_server: SqlServerConfig,
    pub postgres: PostgresConfig,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self, String> {
        if !path.exists() {
            let default_config = Config {
                sql_server: SqlServerConfig {
                    username: "xxxxxxxxxxxxxxxx".to_string(),
                    password: "xxxxxxxxxxxxxxx".to_string(),
                    database: "xxxxxxxxxxxxxx".to_string(),
                    host: "xxxxxxxxxxxxxxxxxxxx".to_string(),
                    port: "xxxxxxxxxxxxxxxxxx".to_string(),
                },
                postgres: PostgresConfig {
                    username: "xxxxxxxxxxxxxxxxxx".to_string(),
                    password: "xxxxxxxxxxxxxxx".to_string(),
                    database: "xxxxxxxxxxxxxxx".to_string(),
                    host: "xxxxxxxxxxxxx".to_string(),
                    port: "xxxxxxxxxxxxxxxxxx".to_string(),
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
