mod extract;
mod load;
mod transform;

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
async fn main() {
    use extract;
    use load;
    use transform;
    use mylog::{error, logs};
    use std::path::PathBuf;

    if let Err(error) = logs::init("logs".to_string(), "1MB".to_string(), "7days".to_string()) {
        panic!("{}", error)
    }

    match load::duckdb::new_connection(None) {
        Ok(connection) => {
            let folder_path = PathBuf::from("data/DVF/extracted");

            if let Err(message) = load::duckdb::from_folder(&connection, &folder_path, "mutations", "Mutations") {
                panic!("........................")
            }

            if let Err(message) = load::duckdb::from_folder(&connection, &folder_path, "classes", "Classes") {
                panic!(".......................")
            }

            if let Err(message) = transform::duckdb::remove_duplicates_mutations(&connection) {
                panic!(".......................")
            }

            if let Err(message) = load::duckdb::export_to_csv(&connection, "data/DVF/mutations.csv", "Mutations") {
                panic!(".......................")
            }

            if let Err(message) = load::duckdb::export_to_csv(&connection, "data/DVF/classes.csv", "Classes") {
                panic!(".......................")
            }
        }
        Err(message) => panic!(".......................")
    }

    /* 
    match extract::api_dvf::main("data/FranceGeoJSON").await {
        Ok(message) => println!("{}", message),
        Err(message) => {
            error!("{}", message);
            eprintln!("{}", message)
        }
    }*/
}
