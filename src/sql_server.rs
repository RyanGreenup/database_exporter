use crate::config::SQLEngineConfig;
use connectorx::prelude::*;
use duckdb::{params, Connection, Result};
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
    // TODO must drop this automagically
    duckdb_conn: Connection,
}

pub trait DatabaseOperations {
    fn new(config: SQLEngineConfig) -> Self
    where
        Self: Sized;
    fn get_optional_tables(&self) -> Vec<Option<String>>;
    fn get_query_all_tables() -> GetTablesQuery;
    fn print_tables(&self);
    fn get_arrow_destination(&self, table: &str, limit: Option<u32>) -> ArrowDestination;
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

    fn get_dataframe(&self, table: &str, limit: Option<u32>) -> DataFrame {
        // Get the arrow Destination
        let destination = self.get_arrow_destination(table, limit);

        // Get a Dataframe (NOTE must have same polars_core version in connectorx
        // and polars, look at `cargo tree | grep polars-core`)
        let df = destination.polars().expect("Unable to get Dataframe");

        return df;
    }
}

impl DatabaseOperations for SQLServer {
    /// See connectorx docs for the mssql docstring
    /// https://sfu-db.github.io/connector-x/databases/mssql.html
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
        // TODO this should take from the toml or the CLI
        // TODO this must respect the extracted path (which should be configurable in the toml
        // TODO this should use an immutable private attribute for the db location
        Self {
            config,
            uri_string: uri,
            source_conn,
            duckdb_conn: Self::make_duckdb_connection(),
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

    fn get_arrow_destination(&self, table: &str, limit: Option<u32>) -> ArrowDestination {
        // Build the query
        let query = match limit {
            Some(n) => format!("SELECT TOP {} * FROM {}", n, table),
            None => format!("SELECT * FROM {}", table),
        };

        // Get the query for the table
        let queries = &[CXQuery::from(&query)];

        // Get a Destination using Arrow
        get_arrow(&self.source_conn, None, queries).expect("Run Failed")
    }
}

/// Represents a parquet file associated with a specific database table.
pub struct TableParquet {
    file_path: PathBuf,
    table_name: String,
}
impl TableParquet {
    pub fn new(table_name: &str) -> Self {
        // TODO choose a directory -- CLI?
        // TODO confirm the directory exists, this should be handled by above mechanism
        // Make a directory called ./parquets/
        // TODO this should be a toml parameter or a CLI Parameter
        let dirname = PathBuf::from("./data/extracted/parquets");
        std::fs::create_dir_all(&dirname).unwrap_or_else(|e| {
            panic!("Unable to create directory: {:?}\n{e}", dirname);
        });

        // Filename
        let mut filename = PathBuf::from(format!("{table_name}.parquet"));
        filename = dirname.join(&filename);

        Self {
            file_path: filename,
            table_name: String::from(table_name),
        }
    }
}

pub trait ExportOperations {
    fn write_to_parquet(&self, table: &str, limit: Option<u32>);
    fn write_parquet_file_to_duckdb_table(&self, parquet_path: &TableParquet, schema: Option<&str>);
}

impl SQLServer {
    // TODO this should not panic so it can be looped
    // Consider returning the path or taking the TableParquet as input
    pub fn write_to_parquet(&self, table: &str, limit: Option<u32>) {
        // Get the dataframe
        let mut df = self.get_dataframe(table, limit);

        // Create the parquet path
        let parquet_path = TableParquet::new(table);
        let filename = parquet_path.file_path;

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

    pub fn export_dataframes(&self, limit: Option<u32>) {
        for maybe_table in self.get_optional_tables() {
            if let Some(table) = maybe_table {
                self.write_to_parquet(&table, limit);
                // TODO set the schema based on the database name
                // Use a variable in the config, e.g. database_name
                let tp = TableParquet::new(&table);
                self.write_parquet_file_to_duckdb_table(&tp, None);
            }
        }
    }

    // TODO I would like to make this a default trait method
    // But I can't because it requires the duckdb_conn
    // Figure this out, maybe make it more general?

    // TODO Export to DuckDB
    // Here we just load the parquets
    // connectorx can't clone in memory which would
    // hit the database again and load the network
    // parquet is memory mapped so it's probably better to do it this way
    // To save memory we should drop the dataframe before getting here
    pub fn write_parquet_file_to_duckdb_table(
        &self,
        parquet_path: &TableParquet,
        schema: Option<&str>,
    ) {
        // Use main by default
        let schema = schema.unwrap_or("main");

        // Change into the directory
        match parquet_path.file_path.to_str() {
            Some(path_str) => {
                // Read the parquet as a file
                self.duckdb_conn
                    .execute(
                        // https://duckdb.org/docs/data/parquet/overview.html
                        &format!(
                            "CREATE OR REPLACE TABLE {schema}.{} AS SELECT * FROM '{}';",
                            &parquet_path.table_name,
                            &path_str.to_string()
                        ),
                        [],
                    )
                    .unwrap_or_else(|e| {
                        panic!(
                            "
                    Unable to read table {} from path {}\n{}
                    ",
                            parquet_path.table_name, path_str, e
                        )
                    });
            }
            None => eprintln!(
                "Unable to get path string from {:?}",
                parquet_path.file_path
            ),
        };
    }
}
