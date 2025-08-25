use std::{
    fs::{self, DirEntry},
    path::PathBuf,
};

use duckdb::Connection;
use mylog::error;

use crate::transform;

const INIT_SCRIPT: &str = include_str!("../../databases/init.sql");
const FILE_PATTERN: &str = "mutations";

/// Create a new connection in memory or in the specified file
fn new_connection(db_path: Option<&str>) -> Result<Connection, ()> {
    match db_path {
        Some(db_path) => Connection::open(db_path).map_err(|e| error!("{}", e)),
        None => Connection::open_in_memory().map_err(|e| error!("{}", e)),
    }
}

fn insert_values(conn: &Connection, path: &str, table_name: &str) -> Result<(), ()> {
    let mut stmt = conn
        .prepare(&format!("TRUNCATE TABLE {};", table_name))
        .map_err(|e| error!("{}", e))?;

    stmt.execute([]).map_err(|e| error!("{}", e))?;

    let mut stmt = conn
        .prepare(&format!(
            "INSERT INTO {} SELECT * FROM read_csv('{}', AUTO_DETECT=TRUE, HEADER=TRUE)",
            table_name, path
        ))
        .map_err(|e| error!("{}", e))?;

    stmt.execute([]).map_err(|e| error!("{}", e))?;

    Ok(())
}

/// Load database from files, apply ***function*** to transform the data and save it
fn from_folder(
    conn: &Connection,
    folder_path: &PathBuf,
    function: impl Fn(&Connection) -> Result<(), ()>,
) -> Result<(), ()> {
    conn.execute_batch(INIT_SCRIPT)
        .map_err(|e| error!("Failed to execute init script : {}", e))?;

    let entries = fs::read_dir(&folder_path)
        .map_err(|e| error!("Failed to read the folder {:?} : {}", folder_path, e))?
        .flatten()
        .collect::<Vec<DirEntry>>();

    for entry in entries {
        let path = entry.path();
        let filename = path.file_name().unwrap().to_string_lossy();

        if filename.starts_with(FILE_PATTERN) && path.extension().unwrap_or_default() == "csv" {
            let path_mutations = path.display().to_string();
            let path_classes = path_mutations.replace("mutations", "classes");

            // Load data from CSV files
            insert_values(&conn, &path_mutations, "mutations")?;
            insert_values(&conn, &path_classes, "classes")?;

            // Transform data
            function(&conn)?;

            // Export transformed data
            export_to_csv(&conn, &path_mutations, "mutations")?;
            export_to_csv(&conn, &path_classes, "classes")?;
        }
    }

    Ok(())
}

/// Export the ***table_name*** into the ***file_path***
fn export_to_csv(conn: &Connection, file_path: &str, table_name: &str) -> Result<(), ()> {
    let file_path = PathBuf::from(file_path);

    let _ = fs::remove_file(&file_path);

    let mut stmt = conn
        .prepare(&format!(
            "COPY {} TO '{:?}' (HEADER, DELIMITER ',')",
            table_name, file_path
        ))
        .map_err(|e| error!("{}", e))?;

    stmt.execute([]).map_err(|e| error!("{}", e))?;

    Ok(())
}

pub fn main(folder_path: &str, db_path: Option<&str>) -> Result<String, String> {
    let conn =
        new_connection(db_path).map_err(|_| "Failed to create a DuckDB connection".to_string())?;

    let folder_path = PathBuf::from(folder_path);

    from_folder(
        &conn,
        &folder_path,
        transform::duckdb::remove_duplicates_mutations,
    )
    .map_err(|_| "Failed to process the data with DuckDB".to_string())?;

    Ok("Successfully transform the data with DuckDB !".to_string())
}
