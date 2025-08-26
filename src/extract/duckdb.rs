use std::{
    fs::{self, DirEntry}, path::PathBuf
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
    /* 
    let mut stmt = conn
        .prepare(&format!("TRUNCATE TABLE {};", table_name))
        .map_err(|e| error!("{}", e))?;

    stmt.execute([]).map_err(|e| error!("{}", e))?;*/

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

    let mut target_folder: PathBuf;
    if let Some(path) = folder_path.parent() {
        target_folder = path.to_path_buf();
    }
    else {
        target_folder = PathBuf::new();
    }
    target_folder.push("cleaned");

    if !fs::exists(&target_folder).unwrap_or(false) {
        let _ = fs::create_dir(&target_folder);
    }

    for entry in entries {
        let path = entry.path();
        let filename = path.file_name().unwrap().to_string_lossy();

        if filename.starts_with(FILE_PATTERN) && path.extension().unwrap_or_default() == "csv" {
            let mutations_src = path.as_os_str().to_string_lossy();
            let classes_src = mutations_src.replace("mutations", "classes");

            // Load data from CSV files
            insert_values(&conn, &mutations_src, "mutations")?;
            insert_values(&conn, &classes_src, "classes")?;

            // Transform data
            function(&conn)?;

            let binding = target_folder.join(filename.as_ref());
            let mutations_dest = binding.as_os_str().to_string_lossy();
            let classes_dest = mutations_dest.replace("mutations", "classes");

            // Export transformed data
            export_to_csv(&conn, &mutations_dest, "mutations")?;
            export_to_csv(&conn, &classes_dest, "classes")?;
        }
    }

    Ok(())
}

/// Export the ***table_name*** into the ***file_path***
fn export_to_csv(conn: &Connection, file_path: &str, table_name: &str) -> Result<(), ()> {
    let file_path = PathBuf::from(file_path);

    fs::File::create(&file_path).map_err(|e| error!("{}", e))?;

    let mut stmt = conn
        .prepare(&format!(
            "COPY {} TO '{}' (HEADER, DELIMITER ',')",
            table_name, &file_path.as_os_str().to_string_lossy()
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
