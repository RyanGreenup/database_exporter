use serde::{Deserialize, Serialize};
use crate::config::SQLEngineConfig;
use crate::database::GetTablesQuery;

/// Represents different types of SQL databases and their specific query formats
/// Eventually this will be replaced with <connectorx::source_router::SourceType>
/// For now not all databases have been implemented
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum DatabaseType {
    SQLServer,
    Postgres,
}
impl DatabaseType {
    /// Creates a connection string for the database type
    /// See connectorx docs for guidance on docstrings:
    ///
    /// * [mssql](https://sfu-db.github.io/connector-x/databases/mssql.html)
    /// * [postgresql](https://sfu-db.github.io/connector-x/databases/postgres.html)
    pub fn create_connection_string(&self, config: &SQLEngineConfig) -> String {
        match self {
            DatabaseType::SQLServer => {
                let mut uri = format!(
                    "mssql://{}:{}@{}:{}/{}",
                    config.username, config.password, config.host, config.port, config.database
                );
                uri = format!("{uri}?encrypt=false");
                uri = format!("{uri}&trusted_connection=false");
                uri = format!("{uri}&trust_server_certificate=true");
                uri
            }
            DatabaseType::Postgres => {
                format!(
                    "postgresql://{}:{}@{}:{}/{}",
                    config.username, config.password, config.host, config.port, config.database
                )
            }
        }
    }

    /// Returns the appropriate query structure for getting all tables in the database
    pub fn get_tables_query(&self) -> GetTablesQuery {
        match self {
            DatabaseType::SQLServer => GetTablesQuery {
                query: r#"
                    SELECT TABLE_NAME as table_name
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_TYPE = 'BASE TABLE' AND
                        TABLE_SCHEMA != 'scratch';"#
                    .to_string(),
                column_name: "table_name".to_string(),
            },
            DatabaseType::Postgres => GetTablesQuery {
                query: r#"
                    SELECT tablename as table_name
                    FROM pg_catalog.pg_tables
                    WHERE schemaname != 'pg_catalog' AND
                        schemaname != 'information_schema';"#
                    .to_string(),
                column_name: "table_name".to_string(),
            },
        }
    }

    /// Returns a query string for getting rows from a specific table
    pub fn get_rows_query(&self, table: &str, limit: Option<u32>) -> String {
        match self {
            DatabaseType::SQLServer => match limit {
                Some(n) => format!("SELECT TOP {} * FROM {}", n, table),
                None => format!("SELECT * FROM {}", table),
            },
            DatabaseType::Postgres => match limit {
                Some(n) => format!("SELECT * FROM {} LIMIT {}", table, n),
                None => format!("SELECT * FROM {}", table),
            },
        }
    }
}
