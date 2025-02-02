use crate::helpers::TableParquet;
use duckdb::Connection;
use std::path::{Path, PathBuf};

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
pub fn write_parquet_files_to_duckdb_table(
    parquet_paths: Vec<TableParquet>,
    schema: &str,
    file_location: &Path,
) -> Result<(), std::error::Error> {
    let schema = &sanitize_schema(schema);

    // Open a connection
    // Fix this error handling AI!
    let duckdb_conn = match Connection::open(PathBuf::from(file_location)) {
        Ok(conn) => conn,
        Err(e) => {
            eprintln!("Unable to open DuckDB Connection\n{e}");
            return Err(e);
        },
    };

    let schema = create_schema(schema, &duckdb_conn);

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
}

pub fn create_schema(schema: &str, conn: &Connection) {
    conn.execute(
        &format!(
            r#"
            SELECT COUNT(*) > 0 AS schema_exists
            FROM information_schema.schemata
            WHERE schema_name = '{}';
            "#,
            &sanitize_schema(schema),
        ),
        [],
    );
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
