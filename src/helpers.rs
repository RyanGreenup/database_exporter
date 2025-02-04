use crate::file_helpers::sanitize_schema;
use std::path::{Path, PathBuf};

/// Represents a parquet file associated with a specific database table.
#[derive(Clone)]
pub struct TableParquet {
    pub file_path: PathBuf,
    pub table_name: String,
}
impl TableParquet {
    pub fn new(table_name: &str, directory: &Path, schema: &str) -> Self {
        Self {
            file_path: build_output_filepath(table_name, directory, schema),
            table_name: String::from(table_name),
        }
    }
}

pub fn build_output_filepath(name: &str, directory: &Path, schema: &str) -> PathBuf {
    let schema = sanitize_schema(schema);
    let dirname = PathBuf::from(directory).join(schema);
    std::fs::create_dir_all(&dirname).unwrap_or_else(|e| {
        panic!("Unable to create directory: {:?}\n{e}", dirname);
    });

    // Filename
    let mut filename = PathBuf::from(format!("{name}.parquet"));
    filename = dirname.join(&filename);
    filename
}
