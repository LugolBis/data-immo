use std::fs::{self, DirEntry, OpenOptions};
use std::io::Read;
use std::path::PathBuf;

use mylog::{error, info};
use regex::Regex;
use reqwest::{Client, Response, header::HeaderMap};
use serde::Serialize;
use serde_json::{Map, Value};
use tokio::time::{Duration, sleep};

use crate::transform::api_dvf::transform_api_data;
use super::geometry::split_geometry;

const FILTERS: [(&str, &str); 3] = [
    ("valeurfonc[lte]", "100000000000000000"),
    ("datemut[lt]", "2025-08-15"),
    ("buffer", "0"),
];

pub const TARGET_FOLDER: &str = "data/DVF/extracted";

/// Return the API Key stored in the ***.env*** at the root
fn get_api_key() -> Result<String, String> {
    let mut buffer = String::new();
    let mut file = OpenOptions::new()
        .read(true)
        .open(".env")
        .map_err(|e| format!("Error occurs when try to read the '.env' file : {}", e))?;

    let _ = file
        .read_to_string(&mut buffer)
        .map_err(|e| format!("Failed to read the '.env' : {}", e))?;
    Ok(buffer)
}

/// Return the API Post Request Response or the Error.
async fn api_post(
    endpoint: &str,
    api_key: &str,
    headers: HeaderMap,
    data: &impl Serialize,
    filters: &impl Serialize,
) -> Result<Response, String> {
    let url = format!(
        "https://api.sogefi-sig.com/{}/dvfplus/v1.0/sogefi/{}",
        api_key, endpoint
    );

    let client = Client::new();

    let response = client
        .post(&url)
        .headers(headers)
        .query(filters)
        .json(data)
        .send()
        .await
        .map_err(|e| format!("Network Error : {}", e))?;

    match response.status().as_u16() {
        200 => Ok(response),
        402 => Err(format!(
            "Failed of POST '{}' - 402 : Ecxceed request quota",
            url
        )),
        status => {
            let reason = response
                .text()
                .await
                .unwrap_or_else(|_| "Impossible to get the error message...".to_string());
            Err(format!(
                "Failed of POST '{}' - {} : {}",
                url, status, reason
            ))
        }
    }
}

/// Get the features from the ***file_path***.
fn get_department(file_path: PathBuf) -> Result<Value, ()> {
    let mut buffer = String::new();
    let mut file = OpenOptions::new()
        .read(true)
        .open(&file_path)
        .map_err(|e| error!("Error occurs when try to read the '.env' file : {}", e))?;

    let _ = file
        .read_to_string(&mut buffer)
        .map_err(|e| error!("Failed to read the '.env' : {}", e))?;

    let value: Value = serde_json::from_str(&buffer).map_err(|e| {
        error!(
            "Failed to convert the '{}' content to a Value : {}",
            file_path.display(),
            e
        )
    })?;
    Ok(value)
}

async fn process_feature(
    feature_id: &str,
    api_key: &str,
    headers: &HeaderMap,
    data: Map<String, Value>,
    regex_error: &Regex,
) -> Result<(), ()> {
    let mut success_count = 0usize;
    let mut failed_retry = false;
    let mut buffer: Vec<Map<String, Value>> = Vec::new();
    buffer.push(data);

    while buffer.len() > 0 {
        let api_response = api_post(
            "mutation/search",
            api_key,
            headers.clone(),
            &buffer.last(),
            &FILTERS,
        )
        .await;

        match (api_response, failed_retry) {
            (Ok(response), _) => {
                let id_generated = format!("{}{}", feature_id, success_count);

                let content = response
                    .text()
                    .await
                    .map_err(|e| error!("Failed to extract the response text : {}", e))?;

                transform_api_data(content, id_generated)?;

                let _ = buffer.pop();
                success_count += 1;
            }
            (Err(message), true) => {
                error!("{} - {}", feature_id, message);
                let _ = buffer.pop();
                failed_retry = false;
            }
            (Err(message), false) => {
                error!("{} - {}", feature_id, message);
                if message.contains("402") || message.contains("501") {
                    let _ = sleep(Duration::from_secs(60));
                    failed_retry = true;
                } else if regex_error.is_match(&message) {
                    let geometry =
                        &buffer
                            .last()
                            .unwrap()
                            .get("geojson")
                            .ok_or(())
                            .map_err(|_| {
                                error!(
                                    "Inconsistant data format whith none 'geojson' key : {:?}",
                                    buffer.last()
                                )
                            })?;

                    match split_geometry(geometry) {
                        Ok((geometry1, geometry2)) => {
                            let mut data1 = Map::new();
                            data1.insert("geojson".to_string(), geometry1);

                            let mut data2 = Map::new();
                            data2.insert("geojson".to_string(), geometry2);

                            let _ = buffer.pop();
                            buffer.push(data1);
                            buffer.push(data2);
                        }
                        Err(message) => {
                            error!(message);
                            let _ = buffer.pop();
                        }
                    }
                } else {
                    let _ = buffer.pop();
                }
            }
        }
    }

    if success_count > 0 { Ok(()) } else { Err(()) }
}

async fn process_features(
    features: Vec<Map<String, Value>>,
    api_key: &str,
    headers: &HeaderMap,
    dpt: usize,
    regex_error: &Regex,
) -> Result<(), ()> {
    for index in 0..features.len() {
        let geometry = features
            .get(index)
            .ok_or(())
            .map_err(|_| error!("Failed to get the feature {}", index))?
            .get("geometry")
            .ok_or(())
            .map_err(|_| error!("The map hasn't any value for the key 'geometry'"))?;

        let mut data = Map::new();
        data.insert("geojson".to_string(), geometry.clone());

        let feature_id = format!("{}{}", dpt, index);

        if let Ok(_) = process_feature(&feature_id, api_key, headers, data, regex_error).await {
            info!(
                "Successfully save the mutation from the dpt {} and feature {}",
                dpt, index
            );
        } else {
            error!(
                "Failed to save the mutation from the dpt {} and feature {}",
                dpt, index
            );
        };
    }
    Ok(())
}

/// Takes as input the folder who's contains the **GeoJSON** files from *'France GeoJSON'*.
pub async fn main(folder_path: &str) -> Result<String, String> {
    let folder_path = PathBuf::from(folder_path);
    let target_folder = PathBuf::from(TARGET_FOLDER);

    if !fs::exists(&target_folder).unwrap_or(false) {
        let _ = fs::create_dir_all(&target_folder).map_err(|e| {
            format!(
                "Failed to create the folder {} : {}",
                target_folder.display(),
                e
            )
        })?;
    }

    let entries = fs::read_dir(&folder_path)
        .map_err(|e| {
            format!(
                "Failed to read the folder {} : {}",
                folder_path.display(),
                e
            )
        })?
        .flatten()
        .collect::<Vec<DirEntry>>();

    let api_key = get_api_key()?;

    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", "application/json".parse().unwrap());
    headers.insert("accept", "application/json".parse().unwrap());

    let regex_error =
        Regex::new(r#"403\s*:\s*\{"message":"Surface\s+(.*?)\s+du\s+GeoJSON\s+trop\s+grande"\}"#)
            .map_err(|e| format!("Failed to initiliaze the regex : {}", e))?;

    let mut dpt = 1usize;
    for entry in entries {
        let path = entry.path();

        if path.is_file() {
            if let Ok(Value::Object(map)) = get_department(path.clone()) {
                let features = map
                    .get("features")
                    .ok_or(format!(
                        "Failed to get the 'features' from {}",
                        path.display()
                    ))?
                    .as_array()
                    .ok_or("Failed to get the Array from the 'features' Value.")?
                    .iter()
                    .map(|v| {
                        if let Value::Object(map) = v {
                            Ok(map.clone())
                        } else {
                            Err(())
                        }
                    })
                    .flatten()
                    .collect::<Vec<Map<String, Value>>>();

                let _ = process_features(features, &api_key, &headers, dpt, &regex_error).await;
                dpt += 1;
            }
        } else {
            info!("Skip the folder : {:?}", entry.path())
        }
    }

    Ok("Successfully run the Data pipeline !".to_string())
}
