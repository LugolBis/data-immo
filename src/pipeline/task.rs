use std::path::PathBuf;

use crate::extract;
use crate::load;
use crate::transform;

pub async fn task1() -> Result<String, String> {
    extract::api_dvf::main("data/FranceGeoJSON").await
}

pub fn task2() -> Result<String, String> {
    match load::duckdb::new_connection(None) {
        Ok(connection) => {
            let folder_path = PathBuf::from("data/DVF/extracted");

            load::duckdb::from_folder(&connection, &folder_path, "mutations", "Mutations")
                .map_err(|_| format!("Failed to extract the mutations from the folder : {:?}", folder_path))?;

            load::duckdb::from_folder(&connection, &folder_path, "classes", "Classes")
                .map_err(|_| format!("Failed to extract the classes from the folder : {:?}", folder_path))?;

            transform::duckdb::remove_duplicates_mutations(&connection)
                .map_err(|_| format!("Failed to remove the duplicates in the table Mutations."))?;

            load::duckdb::export_to_csv(&connection, "data/DVF/mutations.csv", "Mutations")
                .map_err(|_| format!("Failed to export to CSV the table Mutations."))?;

            load::duckdb::export_to_csv(&connection, "data/DVF/classes.csv", "Classes")
                .map_err(|_| format!("Failed to export to CSV the table Classes."))?;

            Ok("Successfully load and transform the data with DuckDB !".to_string())
        }
        Err(_) => Err("Failed to initialize the DuckDB connection in memomry.".to_string())
    }
}