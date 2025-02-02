use crate::helpers::TableParquet;
use duckdb::Connection;
use std::path::PathBuf;

// TODO I would like to make this a default trait method
// But I can't because it requires the duckdb_conn
// Figure this out, maybe make it more general?

// TODO Export to DuckDB
// Here we just load the parquets
// connectorx can't clone in memory which would
// hit the database again and load the network
// parquet is memory mapped so it's probably better to do it this way
// To save memory we should drop the dataframe before getting here
pub fn write_parquet_files_to_duckdb_table(parquet_paths: Vec<TableParquet>, schema: Option<&str>) {
    // Use main by default
    let schema = schema.unwrap_or("main");

    // Open a connection
    // TODO need to figure out how to get the path for the db from CLI or config toml
    let duckdb_conn = Connection::open(PathBuf::from("./data.duckdb"))
        // TODO don't panic!
        .expect("Unable to create duckdb file");

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
                        "ERROR! Unable to read table {} from path {}\n{} ",
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
