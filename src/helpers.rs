use std::path::{Path, PathBuf};

/// Represents a parquet file associated with a specific database table.
#[derive(Clone)]
pub struct TableParquet {
    pub file_path: PathBuf,
    pub table_name: String,
}
impl TableParquet {
    pub fn new(table_name: &str, directory: &Path) -> Self {
        // TODO choose a directory -- CLI?
        // TODO confirm the directory exists, this should be handled by above mechanism
        // Make a directory called ./parquets/
        // TODO this should be a toml parameter or a CLI Parameter
        let dirname = PathBuf::from(directory);
        std::fs::create_dir_all(&dirname).unwrap_or_else(|e| {
            panic!("Unable to create directory: {:?}\n{e}", dirname);
        });

        // Filename
        let mut filename = PathBuf::from(format!("{table_name}.parquet"));
        filename = dirname.join(&filename);

        Self {
            file_path: filename,
            table_name: String::from(table_name),
        }
    }
}
