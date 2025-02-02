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

fn main() {
    let cli = Cli::parse();
    let config_path = cli.get_config_path();

    match SQLEngineConfig::load(&config_path) {
        Ok(configs) => run(
            configs,
            &cli.get_export_directory(),
            cli.database.include_duckdb,
            &cli.database.duckdb_file_name,
            cli.database.row_limit,
        ),
        Err(e) => {
            eprintln!("{}", e);
            process::exit(1);
        }
    }
}

fn run(
    configs: HashMap<String, SQLEngineConfig>,
    export_directory: &Path,
    duckdb_options: Option<DatabaseOptions>,
    row_limit: Option<u32>,
) {
    for (name, config) in configs {
        println!("Processing database: {}", name);

        let db = Database::new(config.clone(), config.database_type);

        // Export all dataframes
        // TODO this should be a toml parameter or a CLI Parameter
        // TODO the config MUST explain to the user if the key is ambiguous

        match db.export_dataframes(row_limit, export_directory, include_duckdb, database_name, &name) {
            Ok(_) => {}
            Err(e) => eprintln!("{e}"),
        }
    }
}
