use connectorx::prelude::*;
use polars::prelude;
use std::convert::TryFrom;

fn get_query_all_tables() -> String {
    return r#"
    SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE = 'BASE TABLE';
    "#
    .into();
}

fn main() {
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

    // Make an arrow
    // let destination = get_arrow(&source_conn, None, queries).expect("run failed");
    // let mut destination = ArrowDestination::new();
    // let dispatcher = Dispatcher::<SQLiteSource, ArrowDestination, SQLiteArrowTransport>::new(source_conn, &mut destination, queries, None);
    // dispatcher.run().expect("run failed");

    // let data = destination.arrow();

    // This is the data
    let destination = get_arrow(&source_conn, None, queries).expect("Run Failed");
    // let data = destination.arrow();
    // TODO Make this a function so we can loop with a log
    let data = destination.polars().expect("Unable to get Dataframe");

    // print it I guess
    println!("{:#?}", data);

    // From here read with duckdb or polars
}
