use super::dbt::{generate_views, run_command};
use super::dremio::{launch_docker_compose, wait_service};

const DBT_COMMANDS: [&str;2] = ["run", "test"];

pub fn main() -> Result<String, String> {
    generate_views("data/DVF")?;

    launch_docker_compose();

    wait_service("http://localhost:9047", 300)
        .map_err(|_| "Failed to connect to the Dremio Service.")?;

    run_command(&[DBT_COMMANDS[0], "--project-dir", "dbt_immo"])?;
    run_command(&[DBT_COMMANDS[1], "--project-dir", "dbt_immo"])?;

    Ok("Successfully Load and Test the data with Dremio and dbt !".to_string())
}
