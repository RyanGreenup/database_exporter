mod cli;
mod config;
mod database;
// TODO these should be merged
mod file_helpers;
mod helpers;
use clap::Parser;
use cli::Cli;
use config::SQLEngineConfig;
use database::Database;
use std::collections::HashMap;
use std::path::Path;
use std::process;
use crate::cli::DuckDBExportOptions;

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

            run(
                configs,
                &cli.get_export_directory(),
                duckdb_options,
                cli.row_limit,
            )
        }
        Err(e) => {
            eprintln!("{}", e);
            process::exit(1);
        }
    }
}

fn run(
    configs: HashMap<String, SQLEngineConfig>,
    export_directory: &Path,
    duckdb_options: Option<DuckDBExportOptions>,
    row_limit: Option<u32>,
) {
    for (name, config) in configs {
        println!("Processing database: {}", name);

        let db = Database::new(config.clone(), config.database_type);

        match db.export_dataframes(
            row_limit,
            export_directory,
            duckdb_options.as_ref(),
            &name,
        ) {
            Ok(_) => {}
            Err(e) => eprintln!("{e}"),
        }
    }
}
