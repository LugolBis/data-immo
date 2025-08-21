mod extract;
mod transform;

#[tokio::main]
async fn main() {
    use extract;
    use mylog::{error, logs};

    if let Err(error) = logs::init("logs".to_string(), "1MB".to_string(), "7days".to_string()) {
        panic!("{}", error)
    }

    match extract::api_dvf::main("data/FranceGeoJSON").await {
        Ok(message) => println!("{}", message),
        Err(message) => {
            error!("{}", message);
            eprintln!("{}", message)
        }
    }
}
