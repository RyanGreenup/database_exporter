# Usage

## Overview

This tool requires a config file to describe the databases details and then uses cli options to describe the target format.

## CLI

The CLI provides a `--help` which should be sufficiently clear, generally the recipe is:

```sh
cargo run -- -c ~/.config/database_exporter/config.toml
```

## Config File
### Overview

The config file takes a list of database connections with a key, this key will become the directory [^1738542073] for the parquets and the schema name in duckdb.

[^1738542073]: `#TODO` I think they're flat right now

For example here are the configurations for a SQL Server, Postgres and SQLite databases [^1738542163]:

[^1738542163]: See also the [Chinook Dataset](https://github.com/lerocha/chinook-database) which is handy for development.

```toml
["Local SQL Server Container"]
username = "sa"
password = "Some(!) G00d P4ssword?"
database = "chinook"
host = "localhost"
port = "1433"
database_type = "sqlserver"

["Local Postgres Container"]
username="postgres"
password="postgres"
database="chinook"
# do I have one here?
host="vidar"
port="5432"
database_type = "postgres"


["Joplin SQLite Database"]
database_type = "sqlite"
database = "/home/ryan/.config/joplin-desktop/database.sqlite"
username=""
password=""
host=""
port=""
```

### Parameters
#### Database Types

The available `database_types` are limited by the available [connector-x sources](https://github.com/sfu-db/connector-x?tab=readme-ov-file#sources), currently implemented is:

> [!WARNING]
> MySQL has not been tested, pull requests welcome.

```rust
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum DatabaseType {
    SQLServer,
    Postgres,
    MySQL,
    SQLite,
}
```

So, for example, `Postgres` would correspond to `database_type=postgres`.
