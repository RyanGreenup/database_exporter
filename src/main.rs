mod cli;
mod config;
mod database;
// TODO these should be merged
mod file_helpers;
mod helpers;
mod postgres;
use clap::Parser;
use cli::Cli;
use config::SQLEngineConfig;
use database::types::DatabaseType;
use database::Database;
use std::process;
use std::collections::HashMap;

fn main() {
    let cli = Cli::parse();
    let config_path = cli.get_config_path();

    match SQLEngineConfig::load(&config_path) {
        Ok(configs) => run(configs),
        Err(e) => {
            eprintln!("{}", e);
            process::exit(1);
        }
    }
}

fn run(configs: HashMap<String, SQLEngineConfig>) {
    for (name, config) in configs {
        println!("Processing database: {}", name);

        let db = Database::new(config.clone(), config.database_type);

        // Export all dataframes
        // TODO this should be a toml parameter or a CLI Parameter
        db.export_dataframes(None);
    }
}
