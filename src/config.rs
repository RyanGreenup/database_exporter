use connectorx::prelude::*;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

struct DatabaseEngine {
    pub sql_server: SQLServer,
    pub postgres: PostgresConfig,
}

struct GetTablesQuery {
    /// The query that will return all tables for the given database
    query: String,
    /// The column with the table names
    column_name: String,
}

#[derive(Debug)]
pub struct SQLServer {
    pub config: SqlServerConfig,
    pub uri_string: String,
    source_conn: SourceConn,
}

// TODO this should become a trait
impl SQLServer {
    pub fn new(config: SqlServerConfig) -> SQLServer {
        // Define the database credentials
        let mut uri = format!(
            "mssql://{}:{}@{}:{}/{}",
            config.username, config.password, config.host, config.port, config.database
        );
        uri = format!("{uri}?encrypt=false");
        uri = format!("{uri}&trusted_connection=false");
        uri = format!("{uri}&trust_server_certificate=true");
        let source_conn = SourceConn::try_from(uri.as_str()).expect("parse conn str failed");
        Self {
            config,
            uri_string: uri,
            source_conn,
        }
    }

    /// A Query to get all tables
    fn get_query_all_tables() -> GetTablesQuery {
        let column_name = "table_name".into();
        let query = format!(
            r#"
        SELECT TABLE_NAME as {}
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE' AND
            TABLE_TYPE != 'scratch';
        "#,
            column_name
        );

        GetTablesQuery { query, column_name }
    }

    // Returns tables, an empty string indicates a missing
    pub fn print_tables(&self) {
        for table in self.get_optional_tables() {
            if let Some(t) = table {
                println!("{t}")
            }
        }
    }

    /// Returns tables as optional values
    pub fn get_optional_tables(&self) -> Vec<Option<String>> {
        // Some Queries
        // let queries = &[CXQuery::from("SELECT * FROM Track")];

        // Get the query for all tables
        let all_tables_query = Self::get_query_all_tables();
        let query = all_tables_query.query;
        let colname = all_tables_query.column_name;

        let queries = &[CXQuery::from(&query)];

        // Get a Destination using Arrow
        let destination = get_arrow(&self.source_conn, None, queries).expect("Run Failed");

        // Get a Dataframe (NOTE must have same polars_core version in connectorx
        // and polars, look at `cargo tree | grep polars-core`)
        let data = destination.polars().expect("Unable to get Dataframe");

        // Print the items
        // TODO we need a struct or Enum
        let col_of_strings = data
            .column(&colname)
            .unwrap_or_else(|e| {
                panic!("Unable to extract heading {colname} from query:\n{query}\n{e}")
            })
            .try_str()
            .unwrap_or_else(|| {
                panic!("Unable to parse column {colname} as strings from query:\n{query}")
            });

        let vec_of_table_names: Vec<Option<String>> = col_of_strings
            .iter()
            .map(|item| {
                if let Some(i) = item {
                    Some(i.to_string())
                } else {
                    eprintln!(
                        "One of the table names was not found, which is unexpected behaviour"
                    );
                    None
                }
            })
            .collect();

        return vec_of_table_names;
    }
}

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
                    username: "".to_string(),
                    password: "".to_string(),
                    database: "".to_string(),
                    host: "".to_string(),
                    port: "".to_string(),
                },
                postgres: PostgresConfig {
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
