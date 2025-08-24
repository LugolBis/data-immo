use std::{
    fs::{self, DirEntry},
    path::PathBuf,
};

use duckdb::Connection;
use mylog::error;

const INIT_SCRIPT: &str = include_str!("../../databases/init.sql");

/// Create a new connection in memory or in the specified file
pub fn new_connection(db_path: Option<&str>) -> Result<Connection, ()> {
    match db_path {
        Some(db_path) => Connection::open(db_path).map_err(|e| error!("{}", e)),
        None => Connection::open_in_memory().map_err(|e| error!("{}", e)),
    }
}

/// Load each file in the ***folder_path*** who start with the ***file_pattern*** into the ***table_name***
pub fn from_folder(conn: &Connection, folder_path: &PathBuf, file_pattern: &str, table_name: &str) -> Result<(), ()> {
    conn.execute_batch(INIT_SCRIPT)
        .map_err(|e| error!("Failed to execute init script : {}", e))?;

    let entries = fs::read_dir(&folder_path)
        .map_err(|e| error!("Failed to read the folder {:?} : {}", folder_path, e))?
        .flatten()
        .collect::<Vec<DirEntry>>();

    for entry in entries {
        let path = entry.path();
        let filename = path.file_name().unwrap().to_string_lossy();

        if filename.starts_with(file_pattern) && path.extension().unwrap_or_default() == "csv" {
            let mut stmt = conn
                .prepare(&format!(
                    "INSERT INTO {} SELECT * FROM read_csv('{}', AUTO_DETECT=TRUE, HEADER=TRUE)",
                    table_name,
                    path.display().to_string()
                ))
                .map_err(|e| error!("{}", e))?;

            stmt.execute([]).map_err(|e| error!("{}", e))?;
        }
    }

    Ok(())
}

/// Export the ***table_name*** into the ***file_path***
pub fn export_to_csv(conn: &Connection, file_path: &str, table_name: &str) -> Result<(), ()> {
    let file_path = PathBuf::from(file_path);

    let _ = fs::remove_file(&file_path);

    let mut stmt = conn.prepare(&format!(
        "COPY {} TO '{:?}' (HEADER, DELIMITER ',')",
        table_name,
        file_path
    )).map_err(|e| error!("{}", e))?;

    stmt.execute([]).map_err(|e| error!("{}", e))?;

    Ok(())
}
