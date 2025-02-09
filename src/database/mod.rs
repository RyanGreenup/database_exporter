pub mod types;

use crate::cli::DuckDBExportOptions;
use crate::config::CustomQuery;
use crate::config::SQLEngineConfig;
#[cfg(feature = "duckdb")]
use crate::file_helpers::write_parquet_files_to_duckdb_table;
#[cfg(feature = "duckdb")]
use crate::file_helpers::DuckDBError;
use crate::helpers::build_output_filepath;
use crate::helpers::TableParquet;
use connectorx::destinations::arrow::ArrowDestinationError;
use connectorx::prelude::*;
use polars::error::PolarsError;
use polars::export::rayon::iter::IntoParallelRefIterator;
use polars::export::rayon::iter::ParallelIterator;
use polars::frame::DataFrame;
use polars::prelude::ParquetWriter;
use std::collections::HashMap;
use std::path::Path;
use types::DatabaseType;

/// Represents errors that can occur during database operations.
///
/// This enum encapsulates various error types that might occur when:
/// - Working with Arrow data structures
/// - Processing DataFrames
/// - Handling Polars operations
/// - Performing I/O operations
/// - Interacting with DuckDB
#[derive(Debug)]
pub enum DatabaseError {
    ArrowError(ConnectorXOutError),
    DataFrameError(ArrowDestinationError),
    PolarsError(PolarsError),
    IoError(std::io::Error),
    #[cfg(feature = "duckdb")]
    DuckDBError(DuckDBError),
}

impl std::fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DatabaseError::ArrowError(e) => write!(f, "Arrow destination error: {e}"),
            DatabaseError::DataFrameError(e) => write!(f, "DataFrame error: {e}"),
            DatabaseError::PolarsError(e) => write!(f, "Polars error: {e}"),
            DatabaseError::IoError(e) => write!(f, "IO Error: {e}"),
            #[cfg(feature = "duckdb")]
            DatabaseError::DuckDBError(e) => {
                write!(f, "Error Loading Parquet Files into DuckDB: {e}")
            }
        }
    }
}

impl std::error::Error for DatabaseError {}

impl From<ConnectorXOutError> for DatabaseError {
    fn from(error: ConnectorXOutError) -> Self {
        DatabaseError::ArrowError(error)
    }
}

impl From<ArrowDestinationError> for DatabaseError {
    fn from(error: ArrowDestinationError) -> Self {
        DatabaseError::DataFrameError(error)
    }
}

impl From<PolarsError> for DatabaseError {
    fn from(error: PolarsError) -> Self {
        DatabaseError::PolarsError(error)
    }
}

impl From<std::io::Error> for DatabaseError {
    fn from(error: std::io::Error) -> Self {
        DatabaseError::IoError(error)
    }
}

#[cfg(feature = "duckdb")]
impl From<DuckDBError> for DatabaseError {
    fn from(error: DuckDBError) -> Self {
        DatabaseError::DuckDBError(error)
    }
}

/// Represents a query for retrieving table information from a database.
///
/// This struct encapsulates both the SQL query string used to retrieve table names
/// and the name of the column that contains the table names in the query results.
pub struct GetTablesQuery {
    /// The query that will return all tables for the given database
    query: String,
    /// The column with the table names
    column_name: String,
}

#[derive(Debug)]
pub struct Database {
    #[allow(dead_code)] // Dead but good for debugging
    pub config: SQLEngineConfig,
    #[allow(dead_code)] // Dead but good for debugging
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
    fn get_arrow_destination(
        &self,
        table: &str,
        limit: Option<u32>,
    ) -> Result<ArrowDestination, ConnectorXOutError> {
        // Build the query
        let query = self.get_table_query(table, limit);

        // Get the query for the table
        let queries = &[CXQuery::from(&query)];

        // Get a Destination using Arrow
        // NOTE this throws an error when using NUMERIC type with sqlite3, use REAL type instead
        get_arrow(self.get_connection(), None, queries)
    }

    /// Get the tables from the database
    fn get_tables(&self) -> Result<Vec<String>, DatabaseError> {
        // Get the query for all tables
        let all_tables_query = self.get_query_all_tables();
        let query = all_tables_query.query;
        let colname = all_tables_query.column_name;

        let queries = &[CXQuery::from(&query)];

        // Get a Destination using Arrow
        let destination =
            get_arrow(self.get_connection(), None, queries).map_err(DatabaseError::from)?;

        // Get a Dataframe
        let data = destination.polars().map_err(DatabaseError::from)?;

        // Extract column and convert to strings
        let col_of_strings = data
            .column(&colname)
            .map_err(DatabaseError::from)?
            .try_str()
            .ok_or_else(|| {
                DatabaseError::PolarsError(PolarsError::ComputeError(
                    format!("Unable to parse column {colname} as strings").into(),
                ))
            })?;

        // Convert to Vec<String>
        let vec_of_table_names: Vec<String> = col_of_strings
            .iter()
            .filter_map(|item| {
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

        Ok(vec_of_table_names)
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

/// Implementation of database operations for connecting to and querying SQL databases.
///
/// This implementation provides methods for:
/// - Creating new database connections
/// - Retrieving and printing table information
/// - Exporting data to Parquet files
/// - Loading data into DuckDB
impl Database {
    /// Creates a new instance of a database connection with the provided configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - The configuration for the SQL engine.
    /// * `db_type` - The type of database to connect to.
    ///
    /// # Returns
    ///
    /// A new instance of the implementing type.
    pub fn new(config: SQLEngineConfig, db_type: DatabaseType) -> Database {
        let uri = db_type.create_connection_string(&config);
        let source_conn = SourceConn::try_from(uri.as_str()).unwrap_or_else(|e| {
            panic!("Unable to connect to database using connection string: {uri}\n{e}")
        });

        Database {
            config,
            uri_string: uri,
            source_conn,
            db_type,
        }
    }

    /// Prints all tables as DataFrames to the console.
    ///
    /// # Arguments
    ///
    /// * `limit` - An optional limit on the number of rows to retrieve from each table.
    #[allow(dead_code)]
    pub fn print_all_tables_as_dataframes(&self, limit: Option<u32>) -> Result<(), DatabaseError> {
        let mut failures = vec![];
        for table in self.get_tables()? {
            match self.get_dataframe(&table, limit) {
                Ok(df) => println!("{:#?}", df),
                Err(e) => failures.push((table.clone(), e)),
            };
            if !failures.is_empty() {
                eprintln!("Unable to print tables: {:#?}", failures);
            }
        }

        Ok(())
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
    pub fn get_dataframe(
        &self,
        table: &str,
        limit: Option<u32>,
    ) -> Result<DataFrame, DatabaseError> {
        // Get the arrow Destination
        let destination = self.get_arrow_destination(table, limit)?;

        // Get a Dataframe (NOTE must have same polars_core version in connectorx
        // and polars, look at `cargo tree | grep polars-core`)

        // Get a Dataframe
        destination.polars().map_err(DatabaseError::from)
    }

    /// Prints the names of all tables to the console.
    #[allow(dead_code)]
    pub fn print_tables(&self) -> Result<(), DatabaseError> {
        for table in self.get_tables()? {
            println!("{table}");
        }
        Ok(())
    }

    /// Retrieves a DataFrame for a given query
    ///
    /// # Arguments
    ///
    /// * `query` - The SQL Query to run
    ///
    /// # Returns
    ///
    /// A DataFrame containing the retrieved data.
    pub fn get_dataframe_from_query(&self, query: &str) -> Result<DataFrame, DatabaseError> {
        // Get the query for the table
        let queries = &[CXQuery::from(&query)];

        // Get a Destination using Arrow
        let destination = get_arrow(self.get_connection(), None, queries)?;

        // Get a Dataframe
        destination.polars().map_err(DatabaseError::from)
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
    pub fn write_to_parquet(
        &self,
        parquet_path: &TableParquet,
        limit: Option<u32>,
    ) -> Result<(), DatabaseError> {
        // Get the dataframe for the table
        let mut df = self.get_dataframe(&parquet_path.table_name, limit)?;

        // Get the standardised filepath
        let filename = &parquet_path.file_path;

        // Write the dataframe to parquet
        write_dataframe_to_parquet(&mut df, filename)?;

        Ok(())
    }

    // get_dataframe_from_query
    /// Writes a SQL Query to a Parquet file.
    ///
    /// # Arguments
    ///
    /// * `` - A reference to a `TableParquet` struct containing the table name and file path.
    /// * `limit` - An optional limit on the number of rows to retrieve from the table.
    pub fn write_query_result_to_parquet(
        &self,
        parquet_path: &Path,
        query: &str,
    ) -> Result<(), DatabaseError> {
        // Get the dataframe for the table
        let mut df = self.get_dataframe_from_query(query)?;

        // Write the dataframe to parquet
        write_dataframe_to_parquet(&mut df, parquet_path)?;

        Ok(())
    }

    /// Exports DataFrames for all tables to Parquet files and loads them into DuckDB.
    ///
    /// # Arguments
    ///
    /// * `limit` - An optional limit on the number of rows to retrieve from each table.
    /// * `export_directory` - A Directory location to export files to
    /// * `include_duckdb` - Whether to include exported duckdb files as well
    /// * `schema` - The schema to use in duckdb
    pub fn export_dataframes(
        &self,
        limit: Option<u32>,
        export_directory: &Path,
        duckdb_options: Option<&DuckDBExportOptions>,
        #[allow(unused_variables)] schema: &str,
        override_limits: Option<HashMap<String, Option<u32>>>,
        custom_queries: Option<Vec<CustomQuery>>,
    ) -> Result<(), DatabaseError> {
        // Get paths to parquet files
        let parquet_paths: Vec<TableParquet> = self
            .get_tables()?
            .into_iter()
            .map(|table_name| TableParquet::new(&table_name, export_directory, schema))
            .collect();

        let mut writable_parquet_paths: Vec<TableParquet> = parquet_paths
            .par_iter()
            .filter_map(|tp| {
                // Check for a row_limit override
                let row_limit = override_limits
                    .as_ref()
                    .and_then(|limits| limits.get(&tp.table_name))
                    .copied() // Convert &Option<u32> to Option<u32>
                    .unwrap_or_else(|| limit);

                // Try (/ Catch) to write the table to a parquet file
                let result =
                    std::panic::catch_unwind(|| match self.write_to_parquet(tp, row_limit) {
                        Ok(_) => Some(tp.clone()),
                        Err(e) => {
                            eprintln!("{e}");
                            None
                        }
                    });

                // Notify the user of an error
                if result.is_err() {
                    println!("Caught a panic on {}", tp.table_name);
                    None // If a panic is caught, we don't include this item.
                } else {
                    result.unwrap()
                }
            })
            .collect();

        // Create custom queries
        if let Some(queries) = custom_queries {
            for query in queries {
                let path = build_output_filepath(&query.name, export_directory, schema);
                match self.write_query_result_to_parquet(&path, &query.query) {
                    Err(e) => {
                        eprintln!("Unable to execute custom query:\n{}\n{}", query.query, e);
                    }
                    Ok(()) => {
                        writable_parquet_paths.extend([TableParquet {
                            file_path: path,
                            table_name: query.name.clone(),
                        }]);
                    }
                }
            }
        }

        #[allow(unused_variables)]
        if let Some(opts) = duckdb_options {
            if cfg!(feature = "duckdb") {
                #[cfg(feature = "duckdb")]
                {
                    // Write to duckdb
                    write_parquet_files_to_duckdb_table(
                        writable_parquet_paths,
                        schema,
                        &export_directory.join(opts.file_name.clone()),
                        opts.separator.as_deref(),
                    )?;
                }
            }
        } else {
            println!("Duckdb Feature is Disabled, No database created");
        }
        Ok(())
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
    #[allow(dead_code)]
    fn write_table_to_parquet_path(
        &self,
        table: &str,
        filename: &Path,
        limit: Option<u32>,
    ) -> Result<(), DatabaseError> {
        // Create all directories
        std::fs::create_dir_all(filename)?;

        // Get the dataframe
        let mut df = self.get_dataframe(table, limit)?;

        // Write the dataframe to parquet
        write_dataframe_to_parquet(&mut df, filename)?;

        Ok(())
    }
}

/// Writes a DataFrame to a Parquet file at the specified path.
///
/// # Arguments
///
/// * `df` - A mutable reference to the DataFrame to write
/// * `filename` - The path where the Parquet file will be written
///
/// # Returns
///
/// A `Result` indicating success or a `DatabaseError` if the write operation fails
pub fn write_dataframe_to_parquet(
    df: &mut DataFrame,
    filename: &Path,
) -> Result<(), DatabaseError> {
    // Write the Parquet File
    let mut file = std::fs::File::create(filename)?;
    ParquetWriter::new(&mut file)
        .finish(df)
        .expect("Unable to write parquet file");
    let mut file = std::fs::File::create(filename)?;

    ParquetWriter::new(&mut file)
        .finish(df)
        .expect("Unable to write parquet file");

    println!("Export Successful for: {:?}!", &filename);

    Ok(())
}
