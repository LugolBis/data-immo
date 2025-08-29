mod extract;
mod load;
mod pipeline;
mod transform;

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
async fn main() {
    use mylog::logs;
    use pipeline::core;

    if let Err(error) = logs::init("logs".to_string(), "1MB".to_string(), "7days".to_string()) {
        panic!("{}", error)
    }

    core::main().await;
}
