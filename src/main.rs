mod cli;
mod config;
mod database;
// TODO these should be merged
mod file_helpers;
mod helpers;
use crate::cli::DuckDBExportOptions;
use clap::Parser;
use cli::Cli;
use config::SQLEngineConfig;
use database::Database;
use std::collections::HashMap;
use std::path::Path;
use std::process;
use std::time::Duration;

fn main() {
    let cli = Cli::parse();
    let config_path = cli.get_config_path();

    match SQLEngineConfig::load(&config_path) {
        Ok(configs) => {
            let duckdb_options = if cli.database.include_duckdb {
                Some(DuckDBExportOptions::from(&cli.database))
            } else {
                None
            };

            run_and_watch(
                configs,
                &cli.get_export_directory(),
                duckdb_options.as_ref(),
                cli.row_limit,
                cli.delay,
            )
        }
        Err(e) => {
            eprintln!("{}", e);
            process::exit(1);
        }
    }
}

/// Continuously monitors and exports data from multiple database configurations.
///
/// # Arguments
///
/// * `configs` - A HashMap of database configurations, keyed by database name
/// * `export_directory` - The directory path where exported files will be saved
/// * `duckdb_options` - Optional DuckDB export configuration
/// * `row_limit` - Optional limit on the number of rows to export per table
/// * `delay` - Optional delay in seconds between export runs
///
/// This function either runs the export once (if no delay is specified) or
/// continuously with a specified delay between runs. Each run processes all
/// configured databases and exports their data to Parquet files.
fn run_and_watch(
    configs: HashMap<String, SQLEngineConfig>,
    export_directory: &Path,
    duckdb_options: Option<&DuckDBExportOptions>,
    row_limit: Option<u32>,
    delay: Option<u32>,
) {
    match delay {
        None => run(configs.clone(), export_directory, duckdb_options, row_limit),
        Some(t) => loop {
            run(configs.clone(), export_directory, duckdb_options, row_limit);
            println!("");
            println!("");
            println!("Export Completed, waiting {t} Seconds before next Run!");
            println!("");
            println!("");
            std::thread::sleep(Duration::from_secs(t.into()));
        },
    }
    // for (name, config) in configs {
    //     println!("Processing database: {}", name);
    //
    //     let db = Database::new(config.clone(), config.database_type);
    //
    //     match db.export_dataframes(row_limit, export_directory, duckdb_options, &name) {
    //         Ok(_) => {}
    //         Err(e) => eprintln!("{e}"),
    //     }
    // }
}

/// Processes and exports data from multiple database configurations.
///
/// # Arguments
///
/// * `configs` - A HashMap of database configurations, keyed by database name
/// * `export_directory` - The directory path where exported files will be saved
/// * `duckdb_options` - Optional DuckDB export configuration
/// * `row_limit` - Optional limit on the number of rows to export per table
///
/// This function iterates through each database configuration, creates a new database
/// connection, and exports the data to Parquet files and optionally to DuckDB.
fn run(
    configs: HashMap<String, SQLEngineConfig>,
    export_directory: &Path,
    duckdb_options: Option<&DuckDBExportOptions>,
    row_limit: Option<u32>,
) {
    for (name, config) in configs {
        println!("Processing database: {}", name);

        let db = Database::new(config.clone(), config.database_type);

        match db.export_dataframes(row_limit, export_directory, duckdb_options, &name) {
            Ok(_) => {}
            Err(e) => eprintln!("{e}"),
        }
    }
}
