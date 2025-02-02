use crate::helpers::TableParquet;
use duckdb::Connection;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub enum DuckDBError {
    ConnectionError(duckdb::Error),
    ExecutionError(duckdb::Error),
    InvalidPathError(String),
}

impl std::fmt::Display for DuckDBError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DuckDBError::ConnectionError(e) => write!(f, "Failed to connect to DuckDB: {}", e),
            DuckDBError::ExecutionError(e) => write!(f, "Failed to execute DuckDB query: {}", e),
            DuckDBError::InvalidPathError(p) => write!(f, "Invalid path provided: {}", p),
        }
    }
}

impl std::error::Error for DuckDBError {}

// TODO I would like to make this a default trait method
// But I can't because it requires the duckdb_conn
// Figure this out, maybe make it more general?

// TODO Export to DuckDB
// Here we just load the parquets
// connectorx can't clone in memory which would
// hit the database again and load the network
// parquet is memory mapped so it's probably better to do it this way
// To save memory we should drop the dataframe before getting here
/// Write parquet files to a duckdb table with an optional schema
/// The schema will be sanitized first
/// Removes the database first
pub fn write_parquet_files_to_duckdb_table(
    parquet_paths: Vec<TableParquet>,
    schema: &str,
    file_location: &Path,
) -> Result<(), DuckDBError> {
    // Remove the File
    remove_database(file_location)?;

    // Sanitize the Schema
    let schema = &sanitize_schema(schema);

    // Open a connection
    // NOTE map to a connection error as PathBuf probably fixed the path
    let duckdb_conn =
        Connection::open(PathBuf::from(file_location)).map_err(DuckDBError::ConnectionError)?;

    create_schema(schema, &duckdb_conn)?;

    for parquet_path in parquet_paths {
        // Change into the directory
        match parquet_path.file_path.to_str() {
            Some(path_str) => {
                // Read the parquet as a file
                // TODO
                // Is this expensive to open?
                // Should we load them all in at once to simplify locking operations
                // Yeah let's collect the paths of duckdb files that need to be loaded in
                // then we can open once and immediately close

                match duckdb_conn.execute(
                    // https://duckdb.org/docs/data/parquet/overview.html
                    &format!(
                        "CREATE OR REPLACE TABLE {schema}.{} AS SELECT * FROM '{}';",
                        &parquet_path.table_name,
                        &path_str.to_string()
                    ),
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

/// Modify a string so it can be a valid duckdb schema
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
