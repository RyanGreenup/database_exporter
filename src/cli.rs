use clap::Parser;
use directories::ProjectDirs;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[clap(about, version, author)]
pub struct Cli {
    /// Path to config file
    #[clap(short, long)]
    config: Option<PathBuf>,
}

impl Cli {
    pub fn get_config_path(&self) -> PathBuf {
        if let Some(path) = &self.config {
            return path.clone();
        }

        // Fall back to XDG config location
        if let Some(proj_dirs) = ProjectDirs::from("com", "yourorg", "chinook-exporter") {
            let config_dir = proj_dirs.config_dir();
            std::fs::create_dir_all(config_dir).expect("Failed to create config directory");
            return config_dir.join("config.toml");
        }

        panic!("Could not determine config file location");
    }
}
