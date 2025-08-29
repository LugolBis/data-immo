use mylog::info;
use reqwest::blocking::Client;
use std::process::Command;
use std::thread;
use std::time::Duration;

pub fn launch_docker_compose() {
    Command::new("docker")
        .args(&["compose", "up", "-d"])
        .spawn()
        .expect("Error when launched Docker Compose");
    info!("Docker Compose launched in background");
}

pub fn wait_service(url: &str, timeout_sec: u64) -> Result<(), ()> {
    let client = Client::new();
    let timeout = Duration::from_secs(timeout_sec);
    let start = std::time::Instant::now();

    info!("Verify the service at {}", url);

    while start.elapsed() < timeout {
        match client.get(url).send() {
            Ok(reponse) if reponse.status().is_success() => {
                info!("Service available !");
                return Ok(());
            }
            _ => {
                info!("Service not available, retry in 3 seconds...");
                thread::sleep(Duration::from_secs(3));
            }
        }
    }
    Err(())
}
