use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DatabaseType {
    SqlServer,
    Postgres,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SQLEngineConfig {
    pub database_type: DatabaseType,
    pub username: String,
    pub password: String,
    pub database: String,
    pub host: String,
    pub port: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub database: SQLEngineConfig,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self, String> {
        if !path.exists() {
            let default_config = Config {
                database: SQLEngineConfig {
                    database_type: DatabaseType::Postgres,
                    username: "postgres".to_string(),
                    password: "postgres".to_string(),
                    database: "chinook".to_string(),
                    host: "localhost".to_string(),
                    port: "5432".to_string(),
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
