#[cfg(feature = "duckdb")]
use crate::helpers::TableParquet;
#[cfg(feature = "duckdb")]
use duckdb::Connection;
#[cfg(feature = "duckdb")]
use std::path::{Path, PathBuf};

#[cfg(feature = "duckdb")]
#[derive(Debug)]
pub enum DuckDBError {
    ConnectionError(duckdb::Error),
    ExecutionError(duckdb::Error),
    InvalidPathError(String),
}

#[cfg(feature = "duckdb")]
impl std::fmt::Display for DuckDBError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DuckDBError::ConnectionError(e) => write!(f, "Failed to connect to DuckDB: {}", e),
            DuckDBError::ExecutionError(e) => write!(f, "Failed to execute DuckDB query: {}", e),
            #[allow(dead_code)]
            DuckDBError::InvalidPathError(p) => write!(f, "Invalid path provided: {}", p),
        }
    }
}

#[cfg(feature = "duckdb")]
impl std::error::Error for DuckDBError {}

/// Writes multiple Parquet files to tables in a DuckDB database.
///
/// # Arguments
///
/// * `parquet_paths` - Vector of TableParquet structs containing file paths and table names
/// * `schema` - The schema name to use in DuckDB (will be sanitized)
/// * `file_location` - Path where the DuckDB database file should be created
///
/// # Returns
///
/// * `Ok(())` if all operations completed successfully
/// * `Err(DuckDBError)` if there were any errors during the process
///
/// # Notes
///
/// - Removes any existing database file at the specified location
/// - Creates the schema if it doesn't exist
/// - Creates or replaces tables for each Parquet file
/// - Tables will be named according to the table names in the TableParquet struct
///
/// # Examples
///
/// ```
/// use std::path::Path;
/// let parquets = vec![
///     TableParquet::new("users", Path::new("./data/users.parquet")),
///     TableParquet::new("orders", Path::new("./data/orders.parquet"))
/// ];
/// write_parquet_files_to_duckdb_table(parquets, "myapp", Path::new("./db.duckdb"))?;
/// ```
///
/// # Considerations
///
/// ConnectorX cannot clone the ArrowDestination in memory so one must either
/// hit the database again to get the data or cache to disk.
/// Given that parquet is already well integrated with duckdb, it's simpler
/// to offload that task to duckdb rather than handle it inernally.
#[cfg(feature = "duckdb")]
pub fn write_parquet_files_to_duckdb_table(
    parquet_paths: Vec<TableParquet>,
    schema: &str,
    file_location: &Path,
    separator: Option<&str>,
) -> Result<(), DuckDBError> {
    // Don't remove the File as this is called for each item in the config
    // This replaces the table anyway, SQLite only writes as needed
    // So this might be kinder to disk usage
    // The caller / user must remove it.
    // remove_database(file_location)?;

    // Sanitize the Schema
    let schema = &sanitize_schema(schema);

    // Choose the separator (i.e. Schema or __ etc.)
    let sep = separator.unwrap_or(".");

    // Open a connection
    // NOTE map to a connection error as PathBuf probably fixed the path
    let duckdb_conn =
        Connection::open(PathBuf::from(file_location)).map_err(DuckDBError::ConnectionError)?;

    // Create the Schema if it doesn't exist
    create_schema(schema, &duckdb_conn)?;

    for parquet_path in parquet_paths {
        // Change into the directory
        match parquet_path.file_path.to_str() {
            Some(path_str) => {
                let query = &format!(
                    // Evaluate whether we want schema or simply __
                    // PITA in the CLI to use schema
                    "CREATE OR REPLACE TABLE {schema}{sep}{} AS SELECT * FROM '{}';",
                    &parquet_path.table_name,
                    &path_str.to_string()
                );
                // println!("{query}");
                match duckdb_conn.execute(
                    // https://duckdb.org/docs/data/parquet/overview.html
                    query,
                    [],
                ) {
                    Ok(_n) => {}
                    Err(e) => eprintln!(
                        "ERROR! Unable to execute SQL Query for table {}\n from path {}\n{}",
                        parquet_path.table_name, path_str, e
                    ),
                }
            }
            None => eprintln!(
                "Unable to get path string from {:?}",
                parquet_path.file_path
            ),
        };
    }

    Ok(())
}

#[cfg(feature = "duckdb")]
pub fn create_schema(schema: &str, conn: &Connection) -> Result<(), DuckDBError> {
    let schema = &sanitize_schema(schema);

    if schema != "main" {
        conn.execute(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"), [])
            .map_err(DuckDBError::ExecutionError)?;
    } else {
        /*
        // First check if schema exists
        let mut stmt = conn
            .prepare(
                "SELECT COUNT(*) > 0 AS schema_exists
             FROM information_schema.schemata
             WHERE schema_name = ?",
            )
            .map_err(DuckDBError::ExecutionError)?;

        let exists: bool = stmt
            .query_row([schema], |row| row.get(0))
            .map_err(DuckDBError::ExecutionError)?;

        if !exists {
            eprintln!("WARNING The main schema does not exist! This is unexpected in duckdb");
        }
        */
    }

    Ok(())
}

/// Sanitizes a schema name to be compatible with DuckDB naming requirements.
///
/// # Arguments
///
/// * `schema` - The schema name to sanitize
///
/// # Returns
///
/// A sanitized string that:
/// - Is converted to lowercase
/// - Starts with a letter (prefixed with 's' if needed)
/// - Contains only alphanumeric characters and underscores
/// - Returns "schema" if input would result in empty string
///
/// # Examples
///
/// ```
/// let sanitized = sanitize_schema("My Schema!");
/// assert_eq!(sanitized, "my_schema_");
///
/// let sanitized = sanitize_schema("123test");
/// assert_eq!(sanitized, "s123test");
///
/// let sanitized = sanitize_schema("");
/// assert_eq!(sanitized, "schema");
/// ```
pub fn sanitize_schema(schema: &str) -> String {
    let sanitized: String = schema
        .chars()
        .enumerate()
        .filter_map(|(i, c)| {
            if i == 0 && !c.is_ascii_alphabetic() {
                Some('s') // Prefix with 's' if doesn't start with letter
            } else if c.is_ascii_alphanumeric() || c == '_' {
                Some(c.to_ascii_lowercase())
            } else {
                Some('_') // Replace special chars with underscore
            }
        })
        .collect();

    if sanitized.is_empty() {
        "schema".to_string() // Default if empty
    } else {
        sanitized
    }
}

/// Attempts to remove a DuckDB database file at the specified location.
///
/// # Arguments
///
/// * `file_location` - Path to the DuckDB database file to remove
///
/// # Returns
///
/// * `Ok(())` if the file was successfully removed or didn't exist
/// * `Err(DuckDBError)` if there was an error removing the file (except for NotFound errors which are ignored)
///
/// # Examples
///
/// ```
/// use std::path::Path;
/// let db_path = Path::new("./my_database.db");
/// remove_database(&db_path)?; // Removes if exists, does nothing if not found
/// ```
#[allow(dead_code)]
#[cfg(feature = "duckdb")]
pub fn remove_database(file_location: &Path) -> Result<(), DuckDBError> {
    // Remove the database if it exists
    match std::fs::remove_file(file_location) {
        Ok(()) => {}
        Err(e) => match e.kind() {
            std::io::ErrorKind::NotFound => {}
            _ => {
                return Err(DuckDBError::InvalidPathError(format!(
                    "Unable to Remove Existing database!\n {e}"
                )))
            }
        },
    }
    Ok(())
}
