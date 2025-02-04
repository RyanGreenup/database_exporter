# Usage

## Overview

This tool requires a config file to describe the databases details and then uses cli options to describe the target format.

## CLI

> [!NOTE]
> Exporting a parquet with 0 rows with `--row-limit=0` will work fine if one only needs the schema

The CLI provides a `--help` which should be sufficiently clear, generally the recipe is:

```sh
# From Source
cargo run -- -c ~/.config/database_exporter/config.toml --row-limit=6 -e data/raw/

# From Binary
./database-export -c ~/.config/database_exporter/config.toml --row-limit=6 -e data/raw/
```

## Config File
> [!NOTE]
> The config file is TOML due to it's excellent support in Rust and human-friendly syntax
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

### Custom Row Limits Override

> [!WARNING]
> There is not yet logic to change the sort order for the custom limit as it was not required for my use case (sufficiently cheap to pull the entire table

If one wants to Override the limit for certain tables, this can be specified in the toml file like so:

```toml
["Joplin SQLite Database".override_limits]
"resources" = 10  # Return first 10 rows
"tags" = -1       # Return all Rows
```

In this example the `resources` table will only return 10 rows, however, the "

### Custom Queries

One can include custom queries like so:

```toml
\[["Joplin SQLite Database".custom_queries]\]
name = "00_test"
description = "A Test Query"
query = "SELECT id FROM notes"

\[["Joplin SQLite Database".custom_queries]\]
name = "01_test"
description = "A Test Query"
query = "SELECT body FROM notes"
```

This will result in two new parquet files: `00_test.parquet` and `01_test.parquet`. This can be useful where the user needs only the most recent data or only an inner join on data, for example the following will return the 10 most recent results:

> [!NOTE]
> Both queries will run, however custom queries run second and clobber any created file.


```toml
["Joplin SQLite Database".override_limits]
"resources" = 0   # Grab Nothing

\[["Joplin SQLite Database".custom_queries]\]
name = "resources"
description = "Get the 10 most recent resources"
query = "SELECT * FROM resources ORDER BY user_updated_time DESC LIMIT 10"

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
