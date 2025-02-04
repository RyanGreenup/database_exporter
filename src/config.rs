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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_is_valid() {
        let default_config = SQLEngineConfig::create_default_config();
        assert!(SQLEngineConfig::validate_config(&default_config).is_ok());
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct CustomQuery {
    /// Basically a Table Name
    pub name: String,
    /// Description of the query
    pub description: String,
    /// The SQL Query
    pub query: String,
}
impl CustomQuery {
    pub fn new(name: &str, description: &str, query: &str) -> Self {
        Self {
            name: name.to_string(),
            description: description.to_string(),
            query: query.to_string(),
        }
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
///
/// # Implementation Notes
///
/// It might have been better to set certain attributes as `Option<>`,
/// Given that they are left black for SQLite, e.g.:
///
/// ```rust
/// pub struct SQLEngineConfig {
///     pub database_type: DatabaseType,
///     pub username: Option<String>,
///     pub password: Option<String>,
///     ...
/// }
/// ```
///
/// However, SQLite is the exception here, one would typically use python
/// to quickly iterate over SQLite and in fact it's likely the target
/// of this very program.
/// We only include SQLite for development purposes and so it's not worth
/// complicating the code when a config validation would be simpler and clearer.
///
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
    override_limits: Option<HashMap<String, TableLimit>>,
    pub custom_queries: Option<Vec<CustomQuery>>,
}

impl SQLEngineConfig {
    pub fn get_override_limits(&self) -> Option<HashMap<String, Option<u32>>> {
        self.override_limits.as_ref().map(|limits| {
            limits
                .iter()
                .map(|(k, v)| {
                    let limit = if v.0 == -1 { None } else { Some(v.0 as u32) };
                    (k.clone(), limit)
                })
                .collect()
        })
    }
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
                override_limits: Some(sqlite_limits),
                custom_queries: Some(vec![
                    CustomQuery::new("00_test", "A Test Query", "SELECT id FROM notes"),
                    CustomQuery::new("01_test", "A Test Query", "SELECT body FROM notes"),
                ]),
            },
        );

        // Create an example for postgres
        default_config.insert(
            "Postgres Database".to_string(),
            SQLEngineConfig {
                database_type: DatabaseType::Postgres,
                username: "postgres".to_string(),
                password: "postgres".to_string(),
                database: String::from("chinook"),
                host: "localhost".to_string(),
                port: "5432".to_string(),
                override_limits: None,
                custom_queries: None,
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
                override_limits: None,
                custom_queries: None,
            },
        );
        println!("{:#?}", default_config);

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
        let config: HashMap<String, SQLEngineConfig> =
            toml::from_str(&contents).map_err(|e| e.to_string())?;
        Self::validate_config(&config)?;
        Ok(config)
    }

    fn validate_config(config: &HashMap<String, SQLEngineConfig>) -> Result<(), String> {
        for (name, engine_config) in config {
            Self::validate_custom_queries(name, engine_config)?;
            match engine_config.database_type {
                DatabaseType::SQLite => {
                    // SQLite only needs database path
                    if engine_config.database.is_empty() {
                        return Err(format!(
                            "Configuration '{}': SQLite database path cannot be empty",
                            name
                        ));
                    }
                    // SQLite shouldn't have username/password/host/port
                    if !engine_config.username.is_empty()
                        || !engine_config.password.is_empty()
                        || !engine_config.host.is_empty()
                        || !engine_config.port.is_empty()
                    {
                        return Err(format!("Configuration '{}': SQLite should not have username, password, host, or port configured", name));
                    }
                }
                DatabaseType::Postgres => {
                    Self::validate_remote_sql_server_config(name, engine_config)?;
                }
                DatabaseType::SQLServer => {
                    Self::validate_remote_sql_server_config(name, engine_config)?;
                }
                DatabaseType::MySQL => {
                    Self::validate_remote_sql_server_config(name, engine_config)?;
                }
            }
        }
        Ok(())
    }

    fn validate_custom_queries(name: &str, engine_config: &SQLEngineConfig) -> Result<(), String> {
        if let Some(custom_queries) = &engine_config.custom_queries {
            for query in custom_queries {
                if !query
                    .name
                    .chars()
                    .next()
                    .map_or(false, |c| c.is_ascii_alphabetic())
                {
                    return Err(format!(
                     "Configuration '{}': Custom query name '{}' must start with an ASCII letter",
                     name, query.name
                 ));
                }
            }
        }
        Ok(())
    }

    fn validate_remote_sql_server_config(
        name: &str,
        engine_config: &SQLEngineConfig,
    ) -> Result<(), String> {
        if engine_config.username.is_empty() {
            return Err(format!(
                "Configuration '{}': username cannot be empty",
                name
            ));
        }
        if engine_config.password.is_empty() {
            return Err(format!(
                "Configuration '{}': password cannot be empty",
                name
            ));
        }
        if engine_config.database.is_empty() {
            return Err(format!(
                "Configuration '{}': database cannot be empty",
                name
            ));
        }
        if engine_config.host.is_empty() {
            return Err(format!("Configuration '{}': host cannot be empty", name));
        }
        if engine_config.port.is_empty() {
            return Err(format!("Configuration '{}': port cannot be empty", name));
        }
        Ok(())
    }
}
