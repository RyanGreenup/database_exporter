use connectorx::prelude::*;
use polars::prelude::{self, ParquetWriter};
use std::convert::TryFrom;
mod cli;
use cli::Cli;

fn get_query_all_tables() -> String {
    return r#"
    SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE = 'BASE TABLE';
    "#
    .into();
}

fn main() {
    let cli = Cli::parse();
    let config_path = cli.get_config_path();
    println!("Using config file at: {}", config_path.display());
    run()
}

fn run() {
    // Define the database credentials
    // TODO make a class for the credentials for docstrings
    let username = "sa";
    let password = "238923klsdklsdklDSDSDS@!!@";
    let database = "chinook";
    let mut uri = format!("mssql://{username}:{password}@localhost:1433/{database}");
    uri = format!("{uri}?encrypt=false");
    uri = format!("{uri}&trusted_connection=false");
    uri = format!("{uri}&trust_server_certificate=true");

    // Try to make the connection
    let source_conn = SourceConn::try_from(uri.as_str()).expect("parse conn str failed");

    // Some Queries
    // let queries = &[CXQuery::from("SELECT * FROM Track")];
    let queries = &[CXQuery::from(get_query_all_tables().as_str())];

    // This is the data
    let destination = get_arrow(&source_conn, None, queries).expect("Run Failed");
    // let data = destination.arrow();
    // TODO Make this a function so we can loop with a log
    let mut data = destination.polars().expect("Unable to get Dataframe");

    // print it I guess
    println!("{:#?}", data);

    let path = "./table_list.parquet";
    let mut file = std::fs::File::create(path).expect("Unable to create parquet file");
    ParquetWriter::new(&mut file)
        .finish(&mut data)
        .expect("Unable to write parquet file");

    println!("Export Successful!");
    println!("See Output at {path}");
    // From here read with duckdb or polars
}
