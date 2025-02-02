use clap::Parser;
use directories::ProjectDirs;
use std::path::PathBuf;

// SELECT schema_name FROM information_schema.schemata;

// SHOW ALL TABLES

// .tables is useless across schema

// SELECT table_name
// FROM information_schema.tables
// WHERE table_schema = 'schema_name';

#[derive(Parser, Debug)]
#[clap(about, version, author)]
pub struct Cli {
    /// Path to config file
    #[clap(short, long)]
    config: Option<PathBuf>,

    /// Export Directory
    #[arg(default_value_t = String::from("./data/extracted/parquets"), short, long)]
    export_directory: String,

    #[command(flatten)]
    #[clap(next_help_heading = "Database Options")]
    pub database: DatabaseOptions,

    /// Limit the number of rows exported per table
    #[arg(long)]
    pub row_limit: Option<u32>,


    /// Run as a service, periodically fetching data (seconds)
    #[arg(long)]
    pub delay: Option<u32>
}


#[derive(Parser, Debug)]
pub struct DatabaseOptions {
    /// Create Duckdb from all Parquet files
    #[arg(default_value_t = true, short, long)]
    pub include_duckdb: bool,

    /// Database Name for duckdb export, this will be underneath the export directory
    #[arg(default_value_t = String::from("database.duckdb"), short, long)]
    pub duckdb_file_name: String,

    /// Custom separator to use instead of schemas in database
    #[arg(long)]
    separator: Option<String>

}

#[derive(Debug, Clone)]
pub struct DuckDBExportOptions {
    pub file_name: String,
    pub separator: Option<String>,
}

impl From<&DatabaseOptions> for DuckDBExportOptions {
    fn from(opts: &DatabaseOptions) -> Self {
        Self {
            file_name: opts.duckdb_file_name.clone(),
            separator: opts.separator.clone(),
        }
    }
}

impl Cli {
    pub fn get_config_path(&self) -> PathBuf {
        if let Some(path) = &self.config {
            return path.clone();
        }

        // Fall back to XDG config location
        if let Some(proj_dirs) = ProjectDirs::from("", "", "database_exporter") {
            let config_dir = proj_dirs.config_dir();
            println!("{:#?}", config_dir);
            std::fs::create_dir_all(config_dir).expect("Failed to create config directory");
            return config_dir.join("config.toml");
        }

        panic!("Could not determine config file location");
    }

    pub fn get_export_directory(&self) -> PathBuf {
        let path = PathBuf::from(self.export_directory.clone());

        std::fs::create_dir_all(&path)
            .unwrap_or_else(|e| panic!("Unable to create directory: {:?}\n{e}", &path));

        path
    }
}

/*

#[derive(Clone, Debug)]
struct Limit {
    value: Option<u32>,
}

impl Limit {
    pub fn default() -> Self {
        Limit { value: None }
    }
}

impl std::fmt::Display for Limit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.value {
            Some(s) => write!(f, "{s}"),
            None => write!(f, "Unlimited"),
        }
    }
}
*/
