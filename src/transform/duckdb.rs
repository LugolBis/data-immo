use duckdb::Connection;
use mylog::error;

const CLEAN_SCRIPT: &str = include_str!("../../databases/remove_duplicates.sql");

pub fn remove_duplicates_mutations(conn: &Connection) -> Result<(), ()> {
    let mut stmt = conn.prepare(CLEAN_SCRIPT).map_err(|e| error!("{}", e))?;

    stmt.execute([]).map_err(|e| error!("{}", e))?;
    Ok(())
}
