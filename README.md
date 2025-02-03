# Database Exporter

Database Exporter is a simple CLI tool that can export Postgres and SQL Server databases into a duckdb database and directory of parquet files.

Unlike a python script, this can be compiled to  a binary and be dropped on Linux / Windows / Mac remote server without the need to configure a virtual environment. It can also run periodically to make fetching snapshots easier.


See the [documentation](https://ryangreenup.github.io/database_exporter/) for more details.

## Installation

## Source

```sh
cd $(mktemp -d)
git clone https://github.com/RyanGreenup/database_exporter
cd database_exporter

cargo build --features duckdb --release
```

## Binary

```sh
# Omit SSL if behind proxy
wget --no-check-certificate \
    'https://github.com/RyanGreenup/database_exporter/releases/download/v0.1.0/database-export.exe'

./database-export.exe --row-limit=5 -e data/raw/ -c sirius_db.toml
```
