use std::{
    env, fs::{self, DirEntry, OpenOptions}, io::Write, path::PathBuf, process::Command
};

use mylog::error;

const MODEL_FOLDER: &str = "dbt_immo/models";

fn generate_view(files_path: Vec<String>, view_name: &str) -> Result<(), String> {
    let path = PathBuf::from(MODEL_FOLDER).join(view_name);
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
        .map_err(|e| format!("{}", e))?;

    let query = files_path
        .iter()
        .map(|table| format!(r##"SELECT * FROM DVF."{}" "##, table))
        .collect::<Vec<String>>()
        .join("\nUNION ALL\n");

    file.write_all(query.as_bytes())
        .map_err(|e| format!("{}", e))
}

pub fn generate_views(folder_path: &str) -> Result<String, String> {
    let folder_path = PathBuf::from(folder_path);
    let mut mutations_path: Vec<String> = Vec::new();
    let mut classes_path: Vec<String> = Vec::new();

    let entries = fs::read_dir(&folder_path)
        .map_err(|e| {
            let message = format!("Failed to read the folder {:?} : {} - {:?}", folder_path, e, env::current_dir());
            error!(message);
            message
        })?
        .flatten()
        .collect::<Vec<DirEntry>>();

    for entry in entries {
        let path = entry.path();

        if path.is_file() && path.extension().unwrap_or_default().to_os_string() == "parquet" {
            let filename = path.file_name().unwrap().to_string_lossy();

            if filename.starts_with("mutations") {
                mutations_path.push(filename.into());
            } else if filename.starts_with("classes") {
                classes_path.push(filename.into())
            }
        }
    }

    generate_view(mutations_path, "mutations.sql")?;
    generate_view(classes_path, "classes.sql")?;

    Ok("Successfully generate the dbt models !".to_string())
}

pub fn run_command(args: &[&str]) -> Result<(), String> {
    let dbt_path = PathBuf::from(".venv").join("bin").join("dbt");

    let mut command = Command::new(dbt_path);
    command.args(args);

    let mut child = command.spawn().map_err(|e| {
        format!(
            "Error occured when try to run the command '{:?}' : {}",
            command, e
        )
    })?;

    let exit_status = child
        .wait()
        .map_err(|e| format!("Error occured when waited the Exit Status : {}", e))?;

    if exit_status.success() {
        Ok(())
    } else {
        Err(exit_status.to_string())
    }
}
