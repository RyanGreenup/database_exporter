use connectorx::prelude::*;
mod cli;
mod config;
mod sql_server;
mod postgres;
use clap::Parser;
use cli::Cli;
use config::{Config, SQLEngineConfig};
use sql_server::{DatabaseOperations, SQLServer};
use std::process;

fn main() {
    let cli = Cli::parse();
    let config_path = cli.get_config_path();

    match Config::load(&config_path) {
        Ok(config) => run(config.sql_server),
        Err(e) => {
            eprintln!("{}", e);
            process::exit(1);
        }
    }
}

fn run(sql_config: config::SQLEngineConfig) {
    let ms_db = SQLServer::new(sql_config);

    // Print all the tables
    // ms_db.print_tables()

    // print all dataframes
    // ms_db.print_dataframes();

    // Export all dataframes (1 row)
    // TODO this should be a toml parameter or a CLI Parameter
    ms_db.export_dataframes(None);


}
