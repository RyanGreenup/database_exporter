use crate::database::types::DatabaseType;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SQLEngineConfig {
    pub database_type: DatabaseType,
    pub username: String,
    pub password: String,
    pub database: String,
    pub host: String,
    pub port: String,
}

impl SQLEngineConfig {
    pub fn load(path: &Path) -> Result<HashMap<String, SQLEngineConfig>, String> {
        if !path.exists() {
            let mut default_config = HashMap::new();
            default_config.insert(
                "Default Database".to_string(),
                SQLEngineConfig {
                    database_type: DatabaseType::Postgres,
                    username: "postgres".to_string(),
                    password: "postgres".to_string(),
                    database: "chinook".to_string(),
                    host: "localhost".to_string(),
                    port: "5432".to_string(),
                },
            );

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
