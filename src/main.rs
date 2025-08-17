mod extract;

#[tokio::main]
async fn main() {
    use mylog::{logs, error};
    use extract;

    if let Err(error) = logs::init(
        "logs".to_string(),
        "1MB".to_string(),
        "7days".to_string()
    )
    {
        panic!("{}", error)
    }

    match extract::main("data/FranceGeoJSON").await {
        Ok(message) => println!("{}", message),
        Err(message) => {
            error!("{}", message);
            eprintln!("{}", message)
        }
    }
}
