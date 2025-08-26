use duckdb::Connection;
use mylog::error;

const CLEAN_SCRIPT: &str = include_str!("../../databases/remove_duplicates.sql");

pub fn remove_duplicates_mutations(conn: &Connection) -> Result<(), ()> {
    conn.execute_batch(CLEAN_SCRIPT)
        .map_err(|e| error!("{}", e))
}
