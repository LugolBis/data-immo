use crate::extract;

pub async fn task1() -> Result<String, String> {
    extract::api_dvf::main("data/FranceGeoJSON").await
}

pub fn task2() -> Result<String, String> {
    extract::duckdb::main("data/DVF/extracted", None)
}
