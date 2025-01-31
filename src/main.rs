use connectorx::prelude::*;
use polars::prelude::ParquetWriter;
use std::convert::TryFrom;
mod cli;
mod config;
use clap::Parser;
use cli::Cli;
use config::{Config, SQLServer};
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

fn run(sql_config: config::SqlServerConfig) {
    let ms_db = SQLServer::new(sql_config);
    ms_db.print_tables()
}
