use crate::database::types::DatabaseType;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableLimit(i32);

impl Default for TableLimit {
    fn default() -> Self {
        TableLimit(-1) // -1 indicates no limit by default
    }
}

/// Configuration for connecting to a SQL database engine.
///
/// This struct holds all necessary connection parameters for various SQL database types
/// including PostgreSQL, SQLite, and SQL Server. Different fields may be used
/// depending on the database type (e.g., SQLite only needs the database path).
///
/// # Examples
///
/// ```
/// use database_exporter::config::SQLEngineConfig;
/// use database_exporter::database::types::DatabaseType;
///
/// let config = SQLEngineConfig {
///     database_type: DatabaseType::Postgres,
///     username: "postgres".to_string(),
///     password: "postgres".to_string(),
///     database: "mydb".to_string(),
///     host: "localhost".to_string(),
///     port: "5432".to_string(),
/// };
/// ```
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SQLEngineConfig {
    pub database_type: DatabaseType,
    pub username: String,
    pub password: String,
    #[serde(default)]
    pub database: String, // Filepath for sqlite
    pub host: String,
    pub port: String,
    #[serde(default)]
    pub override_limits: HashMap<String, TableLimit>,
}

impl SQLEngineConfig {
    fn create_default_config() -> HashMap<String, SQLEngineConfig> {
        let mut default_config = HashMap::new();

        // Create an example for sqlite with table limits
        let mut sqlite_limits = HashMap::new();
        sqlite_limits.insert("resources".to_string(), TableLimit(10));
        sqlite_limits.insert("tags".to_string(), TableLimit(-1));

        default_config.insert(
            "Local SQLite Database".to_string(),
            SQLEngineConfig {
                database_type: DatabaseType::SQLite,
                username: String::new(),
                password: String::new(),
                database: "/database.sqlite".to_string(),
                host: String::new(),
                port: String::new(),
                override_limits: sqlite_limits,
            },
        );

        // Create an example for postgres
        default_config.insert(
            "Postgres Database".to_string(),
            SQLEngineConfig {
                database_type: DatabaseType::Postgres,
                username: "postgres".to_string(),
                password: "postgres".to_string(),
                database: String::new(),
                host: "localhost".to_string(),
                port: "5432".to_string(),
                override_limits: HashMap::new(),
            },
        );

        // Create an example for sqlserver
        default_config.insert(
            "SQL Server Database".to_string(),
            SQLEngineConfig {
                database_type: DatabaseType::SQLServer,
                username: "sa".to_string(),
                password: "Some Good (!) P455w0rd!".to_string(),
                database: "chinook".to_string(),
                host: "localhost".to_string(),
                port: "1433".to_string(),
                override_limits: HashMap::new(),
            },
        );

        default_config
    }

    pub fn load(path: &Path) -> Result<HashMap<String, SQLEngineConfig>, String> {
        if !path.exists() {
            let default_config = Self::create_default_config();
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
