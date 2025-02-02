use crate::config::SQLEngineConfig;
use crate::file_helpers::{write_dataframe_to_parquet, write_parquet_files_to_duckdb_table};
use crate::helpers::TableParquet;
use connectorx::prelude::*;
use duckdb::{params, Connection, Result};
use polars::frame::DataFrame;
use polars::io::parquet;
use polars::prelude::ParquetWriter;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

pub struct GetTablesQuery {
    /// The query that will return all tables for the given database
    query: String,
    /// The column with the table names
    column_name: String,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct SQLServer {
    pub config: SQLEngineConfig,
    pub uri_string: String,
    source_conn: SourceConn,
    // TODO must drop this automagically
    // duckdb_conn: Connection,
}

pub trait DatabaseOperations {
    /// Get the active connection to the database
    fn get_connection(&self) -> &connectorx::source_router::SourceConn;

    /// Construct the Database Object from the config
    fn new(config: SQLEngineConfig) -> Self
    where
        Self: Sized;
    fn get_query_all_tables() -> GetTablesQuery;
    fn make_duckdb_connection() -> Connection {
        Connection::open(PathBuf::from("./data.duckdb")).expect("Unable to create duckdb file")
    }

    fn print_all_tables_as_dataframes(&self, limit: Option<u32>) {
        for maybe_table in self.get_optional_tables() {
            if let Some(table) = maybe_table {
                let df = self.get_dataframe(&table, limit);
                println!("{:#?}", df);
            }
        }
    }

    /// Retrieves the database table as in-memory representation
    /// which can later be transformed into other representations
    fn get_arrow_destination(&self, table: &str, limit: Option<u32>) -> ArrowDestination {
        // Build the query
        let query = match limit {
            Some(n) => format!("SELECT TOP {} * FROM {}", n, table),
            None => format!("SELECT * FROM {}", table),
        };

        // Get the query for the table
        let queries = &[CXQuery::from(&query)];

        // Get a Destination using Arrow
        get_arrow(&self.get_connection(), None, queries).expect("Run Failed")
    }

    fn get_dataframe(&self, table: &str, limit: Option<u32>) -> DataFrame {
        // Get the arrow Destination
        let destination = self.get_arrow_destination(table, limit);

        // Get a Dataframe (NOTE must have same polars_core version in connectorx
        // and polars, look at `cargo tree | grep polars-core`)
        let df = destination.polars().expect("Unable to get Dataframe");

        return df;
    }

    /// Returns tables as optional values
    fn get_optional_tables(&self) -> Vec<Option<String>> {
        // Some Queries
        // let queries = &[CXQuery::from("SELECT * FROM Track")];

        // Get the query for all tables
        let all_tables_query = Self::get_query_all_tables();
        let query = all_tables_query.query;
        let colname = all_tables_query.column_name;

        let queries = &[CXQuery::from(&query)];

        // Get a Destination using Arrow
        let destination = get_arrow(self.get_connection(), None, queries).expect("Run Failed");

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

        vec_of_table_names
    }

    // Returns tables, an empty string indicates a missing
    fn print_tables(&self) {
        for table in self.get_optional_tables() {
            if let Some(t) = table {
                println!("{t}")
            }
        }
    }

    // File Operations
    fn write_to_parquet(&self, parquet_path: &TableParquet, limit: Option<u32>) {
        // Get the dataframe for the table
        let mut df = self.get_dataframe(&parquet_path.table_name, limit);

        // Get the standardised filepath
        let filename = &parquet_path.file_path;

        // Write the dataframe to parquet
        write_dataframe_to_parquet(&mut df, filename);
    }

    fn write_table_to_parquet_path(
        &self,
        table: &str,
        filename: &Path,
        limit: Option<u32>,
    ) -> io::Result<()> {
        // Create all directories
        std::fs::create_dir_all(filename)?;

        // Get the dataframe
        let mut df = self.get_dataframe(table, limit);

        // Write the dataframe to parquet
        write_dataframe_to_parquet(&mut df, filename);

        Ok(())
    }

    fn export_dataframes(&self, limit: Option<u32>) {
        // Get paths to parquet files
        let parquet_paths: Vec<TableParquet> = self
            .get_optional_tables()
            // Consume the original vector
            .into_iter()
            // filter_map automatically drops None
            .filter_map(|maybe_table_name| maybe_table_name)
            // Cast to TableParquet which generates a file path
            .map(|table_name| TableParquet::new(&table_name))
            // Collect into an iterator
            .collect();

        // Write to files
        for tp in &parquet_paths {
            self.write_to_parquet(&tp, limit);
        }

        // Write to duckdb
        write_parquet_files_to_duckdb_table(parquet_paths, None);
    }
}

impl DatabaseOperations for SQLServer {
    fn get_connection(&self) -> &connectorx::source_router::SourceConn {
        &self.source_conn
    }

    /// See connectorx docs for the mssql docstring
    /// https://sfu-db.github.io/connector-x/databases/mssql.html
    fn new(config: SQLEngineConfig) -> SQLServer {
        // Define the database credentials
        // TODO this could be DRYer
        let mut uri = format!(
            "mssql://{}:{}@{}:{}/{}",
            config.username, config.password, config.host, config.port, config.database
        );
        uri = format!("{uri}?encrypt=false");
        uri = format!("{uri}&trusted_connection=false");
        uri = format!("{uri}&trust_server_certificate=true");
        let source_conn = SourceConn::try_from(uri.as_str()).expect("parse conn str failed");
        // TODO this should take from the toml or the CLI
        // TODO this must respect the extracted path (which should be configurable in the toml
        // TODO this should use an immutable private attribute for the db location
        Self {
            config,
            uri_string: uri,
            source_conn,
            // duckdb_conn: Self::make_duckdb_connection(),
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
            TABLE_SCHEMA != 'scratch';
        "#,
            column_name
        );

        GetTablesQuery { query, column_name }
    }
}
