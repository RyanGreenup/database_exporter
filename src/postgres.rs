/*
#[derive(Debug)]
#[allow(dead_code)]
pub struct Postgres {
    pub config: SQLEngineConfig,
    pub uri_string: String,
    source_conn: SourceConn,
}

impl Postgres {
    /// The URI string used by connectorx
    /// See the documentation at <https://sfu-db.github.io/connector-x/databases/postgres.html>
    fn new(config: SQLEngineConfig) -> Self {
        // Define the database credentials
        let mut uri = format!(
            "postgres://{}:{}@{}:{}/{}",
            config.username, config.password, config.host, config.port, config.database
        );
        let source_conn = SourceConn::try_from(uri.as_str()).expect("parse conn str failed");
        Self {
            config,
            uri_string: uri,
            source_conn,
        }
    }

    /// A Query to get all tables
    fn get_query_all_tables() -> GetTablesQuery {
        let column_name = "table_name".into();
        let query = format!(
            // \dt
            r#"
        SELECT {}
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_type = 'BASE TABLE';
        "#,
            column_name
        );

        GetTablesQuery { query, column_name }
    }

    // TODO how to implement other methods and remain dry?

    fn write_table_to_duckdb(table: &str, head: u32) {
    }



}
*/
