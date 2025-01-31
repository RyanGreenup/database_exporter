use crate::config::SQLEngineConfig;
use connectorx::prelude::*;
use polars::frame::DataFrame;
use polars::prelude::ParquetWriter;
use serde::{Deserialize, Serialize};
use std::fs;
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
}

pub trait DatabaseOperations {
    fn new(config: SQLEngineConfig) -> Self
    where
        Self: Sized;
    fn get_optional_tables(&self) -> Vec<Option<String>>;
    fn get_query_all_tables() -> GetTablesQuery;
    fn print_tables(&self);
}

impl DatabaseOperations for SQLServer {
    fn new(config: SQLEngineConfig) -> SQLServer {
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
            TABLE_SCHEMA != 'scratch';
        "#,
            column_name
        );

        GetTablesQuery { query, column_name }
    }

    // Returns tables, an empty string indicates a missing
    fn print_tables(&self) {
        for table in self.get_optional_tables() {
            if let Some(t) = table {
                println!("{t}")
            }
        }
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

        vec_of_table_names
    }
}

impl SQLServer {
    // AI: get_dataframe is defined here
    pub fn get_dataframe(&self, table: &str, head: u32) -> DataFrame {
        // Get the query for the table
        let query = format!("SELECT TOP {} * FROM {}", head, table);
        let queries = &[CXQuery::from(&query)];

        // Get a Destination using Arrow
        let destination = get_arrow(&self.source_conn, None, queries).expect("Run Failed");

        // Get a Dataframe (NOTE must have same polars_core version in connectorx
        // and polars, look at `cargo tree | grep polars-core`)
        let df = destination.polars().expect("Unable to get Dataframe");

        return df;
    }

    // TODO this should not panic so it can be looped
    pub fn write_to_parquet(&self, table: &str, head: u32) {
        // Get the dataframe
        let mut df = self.get_dataframe(table, head);

        // Make a directory called ./parquets/
        // TODO this should be a toml parameter or a CLI Parameter
        let dirname = PathBuf::from("./data/extracted/parquets");
        let dir = std::fs::create_dir_all(&dirname).unwrap_or_else(|e| {
            panic!("Unable to create directory: {:?}\n{e}", dirname);
        });

        // Filename
        let mut filename = PathBuf::from(format!("{table}.parquet"));
        filename = dirname.join(&filename);

        // Write the Parquet File
        let mut file = std::fs::File::create(&filename).expect("Unable to create parquet file");
        ParquetWriter::new(&mut file)
            .finish(&mut df)
            .expect("Unable to write parquet file");
        let mut file = std::fs::File::create(&filename).expect("Unable to create parquet file");

        ParquetWriter::new(&mut file)
            .finish(&mut df)
            .expect("Unable to write parquet file");

        println!("Export Successful for: {:?}!", &filename);
    }

    pub fn print_dataframes(&self) {
        for maybe_table in self.get_optional_tables() {
            if let Some(table) = maybe_table {
                let df = self.get_dataframe(&table, 2);
                println!("{:#?}", df);
            }
        }
    }

    pub fn export_dataframes(&self, head: u32) {
        for maybe_table in self.get_optional_tables() {
            if let Some(table) = maybe_table {
                self.write_to_parquet(&table, head)
            }
        }
    }


    // TODO Export to DuckDB
}
