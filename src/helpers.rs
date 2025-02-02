use std::path::PathBuf;

use polars::frame::DataFrame;

/// Represents a parquet file associated with a specific database table.
pub struct TableParquet {
    pub file_path: PathBuf,
    pub table_name: String,
}
impl TableParquet {
    pub fn new(table_name: &str) -> Self {
        // TODO choose a directory -- CLI?
        // TODO confirm the directory exists, this should be handled by above mechanism
        // Make a directory called ./parquets/
        // TODO this should be a toml parameter or a CLI Parameter
        let dirname = PathBuf::from("./data/extracted/parquets");
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

pub struct DataFrameTable {
    pub parquet_file_path: PathBuf,
    pub table_name: String,
    pub dataframe: DataFrame,
}

