use crate::config::SQLEngineConfig;
use crate::file_helpers::{write_dataframe_to_parquet, write_parquet_files_to_duckdb_table};
use crate::helpers::TableParquet;
use connectorx::prelude::*;
use polars::frame::DataFrame;
use std::io;
use std::path::Path;

pub struct GetTablesQuery {
    /// The query that will return all tables for the given database
    query: String,
    /// The column with the table names
    column_name: String,
}

/// Represents different types of SQL databases and their specific query formats
#[derive(Debug)]
pub enum DatabaseType {
    SQLServer,
    PostgreSQL,
}

impl DatabaseType {
    /// Creates a connection string for the database type
    pub fn create_connection_string(
        &self,
        config: &SQLEngineConfig,
    ) -> String {
        match self {
            /// See connectorx docs for the mssql docstring
            /// https://sfu-db.github.io/connector-x/databases/mssql.html
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
            DatabaseType::PostgreSQL => {
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
            DatabaseType::PostgreSQL => GetTablesQuery {
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
            DatabaseType::PostgreSQL => match limit {
                Some(n) => format!("SELECT * FROM {} LIMIT {}", table, n),
                None => format!("SELECT * FROM {}", table),
            },
        }
    }
}

#[derive(Debug)]
pub struct Database {
    pub config: SQLEngineConfig,
    uri_string: String,
    source_conn: SourceConn,
    db_type: DatabaseType,
}

/// Provides internal operations for interacting with a SQL Server database.
///
/// This trait defines methods that are used internally by the `SQLServer` struct
/// to manage database connections and retrieve table information.
trait InternalDatabaseOperations {
    /// Returns a reference to the database connection.
    fn get_connection(&self) -> &connectorx::source_router::SourceConn;

    // TODO create an enum of structs that contain the queries all in one place?

    /// Returns the query to retrieve all table names from the database.
    ///
    /// # Returns
    ///
    /// A `GetTablesQuery` struct containing the SQL query and the column name for table names.
    fn get_query_all_tables(&self) -> GetTablesQuery;

    /// Returns the query to retrieve data from a specific table with an optional row limit.
    ///
    /// # Arguments
    ///
    /// * `table` - The name of the table to retrieve data from.
    /// * `limit` - An optional limit on the number of rows to retrieve.
    ///
    /// # Returns
    ///
    /// A SQL query string for retrieving data from the specified table with an optional row limit.
    fn get_table_query(&self, table: &str, limit: Option<u32>) -> String;

    /// Retrieves an ArrowDestination for a given table with an optional row limit.
    /// The ArrowDestination is an in-memory representation
    /// which can later be transformed into other useful representations.
    /// Note, that ArrowDestination does not implement clone and
    /// later transformations take ownership, so this is only needed internally
    ///
    /// # Arguments
    ///
    /// * `table` - The name of the table to retrieve data from.
    /// * `limit` - An optional limit on the number of rows to retrieve.
    ///
    /// # Returns
    ///
    /// An ArrowDestination containing the retrieved data.
    fn get_arrow_destination(&self, table: &str, limit: Option<u32>) -> ArrowDestination {
        // Build the query
        let query = self.get_table_query(table, limit);

        // Get the query for the table
        let queries = &[CXQuery::from(&query)];

        // Get a Destination using Arrow
        get_arrow(&self.get_connection(), None, queries).expect("Run Failed")
    }

    /// Get the tables from the database
    fn get_tables(&self) -> Vec<String> {
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

        // TODO we need a struct or Enum
        let col_of_strings = data
            .column(&colname)
            .unwrap_or_else(|e| {
                panic!("Unable to extract column: {colname} from query:\n{query}\n{e}")
            })
            .try_str()
            .unwrap_or_else(|| {
                panic!("Unable to parse column {colname} as strings from query:\n{query}")
            });

        let vec_of_table_names: Vec<String> = col_of_strings
            .iter()
            .filter_map(|item| {
                if let Some(i) = item {
                    Some(i.to_string())
                } else {
                    // Let the user know so it can be investigated as this is unexpected
                    eprintln!(
                        "One of the table names was not found, which is unexpected behaviour"
                    );
                    // Filter map automatically removes None Values
                    None
                }
            })
            .collect();

        vec_of_table_names
    }
}

/// Provides public operations for interacting with a Connector-X database
///
/// This trait extends `InternalDatabaseOperations` and provides additional methods
/// for common tasks such as printing tables, retrieving dataframes, writing to Parquet,
/// and exporting dataframes to DuckDB.
pub trait PublicDatabaseOperations: InternalDatabaseOperations {
    /// Creates a new instance of SQLServer with the provided configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - The configuration for the SQL engine.
    /// * `db_type` - The type of database to connect to.
    ///
    /// # Returns
    ///
    /// A new instance of SQLServer.
    fn new(config: SQLEngineConfig, db_type: DatabaseType) -> Self
    where
        Self: Sized;

    /// Prints all tables as DataFrames to the console.
    ///
    /// # Arguments
    ///
    /// * `limit` - An optional limit on the number of rows to retrieve from each table.
    fn print_all_tables_as_dataframes(&self, limit: Option<u32>) {
        for table in self.get_tables() {
            let df = self.get_dataframe(&table, limit);
            println!("{:#?}", df);
        }
    }

    /// Retrieves a DataFrame for a given table with an optional row limit.
    ///
    /// # Arguments
    ///
    /// * `table` - The name of the table to retrieve data from.
    /// * `limit` - An optional limit on the number of rows to retrieve.
    ///
    /// # Returns
    ///
    /// A DataFrame containing the retrieved data.
    fn get_dataframe(&self, table: &str, limit: Option<u32>) -> DataFrame {
        // Get the arrow Destination
        let destination = self.get_arrow_destination(table, limit);

        // Get a Dataframe (NOTE must have same polars_core version in connectorx
        // and polars, look at `cargo tree | grep polars-core`)
        let df = destination.polars().expect("Unable to get Dataframe");

        return df;
    }

    /// Prints the names of all tables to the console.
    fn print_tables(&self) {
        for table in self.get_tables() {
            println!("{table}");
        }
    }

    /*
    // File Operations ........................................................
     */

    /// Writes a DataFrame to a Parquet file.
    ///
    /// # Arguments
    ///
    /// * `parquet_path` - A reference to a `TableParquet` struct containing the table name and file path.
    /// * `limit` - An optional limit on the number of rows to retrieve from the table.
    fn write_to_parquet(&self, parquet_path: &TableParquet, limit: Option<u32>) {
        // Get the dataframe for the table
        let mut df = self.get_dataframe(&parquet_path.table_name, limit);

        // Get the standardised filepath
        let filename = &parquet_path.file_path;

        // Write the dataframe to parquet
        write_dataframe_to_parquet(&mut df, filename);
    }

    /// Writes a DataFrame for a given table to a specified Parquet file path.
    ///
    /// # Arguments
    ///
    /// * `table` - The name of the table to retrieve data from.
    /// * `filename` - A reference to the `Path` where the Parquet file will be written.
    /// * `limit` - An optional limit on the number of rows to retrieve from the table.
    ///
    /// # Returns
    ///
    /// An `io::Result<()>` indicating success or failure.
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

    /// Exports DataFrames for all tables to Parquet files and loads them into DuckDB.
    ///
    /// # Arguments
    ///
    /// * `limit` - An optional limit on the number of rows to retrieve from each table.
    fn export_dataframes(&self, limit: Option<u32>) {
        // Get paths to parquet files
        let parquet_paths: Vec<TableParquet> = self
            .get_tables()
            // Consume the original vector
            .into_iter()
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

impl InternalDatabaseOperations for Database {
    fn get_connection(&self) -> &connectorx::source_router::SourceConn {
        &self.source_conn
    }

    fn get_table_query(&self, table: &str, limit: Option<u32>) -> String {
        self.db_type.get_rows_query(table, limit)
    }

    fn get_query_all_tables(&self) -> GetTablesQuery {
        self.db_type.get_tables_query()
    }
}

impl PublicDatabaseOperations for Database {
    fn new(config: SQLEngineConfig, db_type: DatabaseType) -> Self {
        let uri = db_type.create_connection_string(&config);
        let source_conn = SourceConn::try_from(uri.as_str()).expect("parse conn str failed");
        
        Self {
            config,
            uri_string: uri,
            source_conn, 
            db_type,
        }
    }
}
