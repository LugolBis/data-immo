mod tables;

use std::path::PathBuf;

use csv::Writer;
use mylog::error;
use serde::Serialize;
use serde_json::{self, Map, Value};

use crate::extract::TARGET_FOLDER;
use tables::{Classes, Mutation, SharedMutationProps};

fn map_parcelles(
    parcelles: Vec<Map<String, Value>>,
    shared_props: &SharedMutationProps,
    valeurfonc: f64,
    idmutation: u64,
    id_generated: &str,
    id_parcelle: &mut u64,
    mutations: &mut Vec<Mutation>,
    classes: &mut Vec<Classes>,
) -> Result<(), ()> {
    for parcelle in parcelles {
        let dcnt = parcelle
            .get("dcnt")
            .ok_or(())
            .map_err(|_| error!("Failed to get the value of the key 'dcnt'"))?
            .as_array()
            .ok_or(())
            .map_err(|_| error!("Inconsistant value : Expected a Array<Value>"))?;

        *id_parcelle += 1;

        let id = format!("{}{}", id_generated, id_parcelle)
            .parse::<u64>()
            .map_err(|e| error!("id={}{} : {}", id_generated, id_parcelle, e))?;

        let classes_rows = Classes::extract(dcnt, id)?;
        let mutation_row = Mutation::extract(parcelle, shared_props, valeurfonc, idmutation, id)?;

        classes.extend(classes_rows);
        mutations.push(mutation_row);
    }
    Ok(())
}

fn map_dispositions(
    dispositions: &Vec<Value>,
    shared_props: &SharedMutationProps,
    id_generated: &str,
    id_parcelle: &mut u64,
    mutations: &mut Vec<Mutation>,
    classes: &mut Vec<Classes>,
) -> Result<(), ()> {
    let dispositions = dispositions
        .into_iter()
        .map(|v| {
            if let Value::Object(map) = v {
                Ok(map)
            } else {
                Err(())
            }
        })
        .flatten()
        .collect::<Vec<&Map<String, Value>>>();

    for disposition in dispositions {
        let parcelles = disposition
            .get("parcelles")
            .ok_or(())
            .map_err(|_| error!("Failed to get the value of the key 'parcelles'"))?
            .as_array()
            .ok_or(())
            .map_err(|_| error!("Inconsistant value : Expected a Array<Value>"))?;

        let parcelles = parcelles
            .into_iter()
            .map(|v| {
                if let Value::Object(map) = v {
                    Ok(map.clone())
                } else {
                    Err(())
                }
            })
            .flatten()
            .collect::<Vec<Map<String, Value>>>();

        let valeurfonc = disposition
            .get("valeurfonc")
            .ok_or(())
            .map_err(|_| error!("Failed to get the value of the key 'valeurfonc'"))?
            .as_f64()
            .ok_or(())
            .map_err(|_| error!("Inconsistant value : Expected a f64"))?;

        let idmutation = disposition
            .get("idmutation")
            .ok_or(())
            .map_err(|_| error!("Failed to get the value of the key 'idmutation'"))?
            .as_u64()
            .ok_or(())
            .map_err(|_| error!("Inconsistant value : Expected a u64"))?;

        map_parcelles(
            parcelles,
            shared_props,
            valeurfonc,
            idmutation,
            id_generated,
            id_parcelle,
            mutations,
            classes,
        )?;
    }

    Ok(())
}

fn map_properties(
    properties: &Map<String, Value>,
    id_generated: &str,
    id_parcelle: &mut u64,
    mutations: &mut Vec<Mutation>,
    classes: &mut Vec<Classes>,
) -> Result<(), ()> {
    let vefa = properties
        .get("vefa")
        .ok_or(())
        .map_err(|_| error!("Failed to get the value of the key 'vefa'"))?
        .as_bool()
        .ok_or(())
        .map_err(|_| error!("Inconsistant value : Expected a Boolean"))?;

    let datemut = String::from(
        properties
            .get("datemut")
            .ok_or(())
            .map_err(|_| error!("Failed to get the value of the key 'datemut'"))?
            .as_str()
            .ok_or(())
            .map_err(|_| error!("Inconsistant value : Expected a Str"))?,
    );

    let typologie = String::from(
        properties
            .get("typologie")
            .ok_or(())
            .map_err(|_| error!("Failed to get the value of the key 'typologie'"))?
            .as_object().ok_or(())
            .map_err(|_| error!("Inconsistant value : Expected a Map<String, Value>"))?
            .get("libelle").ok_or(())
            .map_err(|_| error!("Failed to get the value of the key 'libelle'"))?
            .as_str()
            .ok_or(())
            .map_err(|_| error!("Inconsistant value : Expected a Str"))?,
    );

    let nature = String::from(
        properties
            .get("nature_mutation")
            .ok_or(())
            .map_err(|_| error!("Failed to get the value of the key 'nature_mutation'"))?
            .as_object().ok_or(())
            .map_err(|_| error!("Inconsistant value : Expected a Map<String, Value>"))?
            .get("libelle").ok_or(())
            .map_err(|_| error!("Failed to get the value of the key 'libelle'"))?
            .as_str()
            .ok_or(())
            .map_err(|_| error!("Inconsistant value : Expected a Str"))?,
    );

    let shared_props = SharedMutationProps::new(vefa, typologie, datemut, nature);

    let dispositions = properties
        .get("dispositions")
        .ok_or(())
        .map_err(|_| error!("Failed to get the value of the key 'dispositions'"))?
        .as_array()
        .ok_or(())
        .map_err(|_| error!("Inconsistant value : Expected a Array<Value>"))?;

    map_dispositions(
        dispositions,
        &shared_props,
        id_generated,
        id_parcelle,
        mutations,
        classes,
    )
}

fn save_transformations(
    path: PathBuf,
    records: Vec<(impl Serialize + std::fmt::Debug)>,
) -> Result<(), ()> {
    let mut writer =
        Writer::from_path(&path).map_err(|e| error!("File path '{:?}' : {}", path, e))?;

    for record in &records {
        writer
            .serialize(record)
            .map_err(|e| error!("Record={:?} : {}", record, e))?;
    }

    writer
        .flush()
        .map_err(|e| error!("Failed to flush : {}", e))?;

    Ok(())
}

pub fn transform_api_data(data: String, id_generated: String) -> Result<(), ()> {
    let value: Value = serde_json::from_str(&data).map_err(|e| {
        error!(
            "Failed to convert the '{}' content to a Value : {}",
            data, e
        )
    })?;

    let features = value
        .as_object()
        .ok_or(())
        .map_err(|_| error!("Inconsistant value : Expected a Value::Object<Map<String, Value>>"))?
        .get("features")
        .ok_or(())
        .map_err(|_| error!("Failed to get the value of the key 'features'"))?
        .as_array()
        .ok_or(())
        .map_err(|_| error!("Inconsistant value : Expected an Array<Value>"))?;

    let mut mutations: Vec<Mutation> = Vec::new();
    let mut classes: Vec<Classes> = Vec::new();
    let mut id_parcelle = 0u64;

    for feature in features {
        let properties = feature
            .as_object()
            .ok_or(())
            .map_err(|_| {
                error!("Inconsistant value : Expected a Value::Object<Map<String, Value>>")
            })?
            .get("properties")
            .ok_or(())
            .map_err(|_| error!("Failed to get the value of the key 'properties'"))?
            .as_object()
            .ok_or(())
            .map_err(|_| {
                error!("Inconsistant value : Expected a Value::Object<Map<String, Value>>")
            })?;

        map_properties(
            properties,
            &id_generated,
            &mut id_parcelle,
            &mut mutations,
            &mut classes,
        )?;

        id_parcelle += 1;
    }

    let folder_path = PathBuf::from(TARGET_FOLDER);
    let mutations_path = folder_path.join(format!("mutations_{}.csv", id_generated));
    let classes_path = folder_path.join(format!("classes_{}.csv", id_generated));

    save_transformations(mutations_path, mutations)?;
    save_transformations(classes_path, classes)?;
    Ok(())
}
