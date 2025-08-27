use mylog::error;
use serde_json::{self, Map, Value};

use super::utils::parse_date;

use arrow::{
    array::{ArrayRef, BooleanArray, Date32Array, Float64Array, StringArray, UInt64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::{fs::File, path::PathBuf};
use std::sync::Arc;

/// Represent the SQL table '***Mutation***'
#[derive(Debug, Clone)]
pub struct Mutation {
    idg: u64,
    idpar: String,
    idmutation: u64,
    shared_props: SharedMutationProps,
    adresse: Adresse,
    valeur_fonciere: f64,
    vendu: bool,
}

#[derive(Debug, Clone)]
pub struct SharedMutationProps {
    vefa: bool,
    typologie: String,
    datemut: String,
    nature: String,
}

#[derive(Debug, Clone)]
pub struct Adresse {
    btq: Option<String>,
    voie: Option<String>,
    novoie: Option<u64>,
    codvoie: Option<String>,
    commune: String,
    typvoie: Option<String>,
    codepostal: String,
}

/// Represent the SQL table '***Classes***'
#[derive(Debug, Clone)]
pub struct Classes {
    idg: u64,
    libelle: String,
    surface: f64,
}

impl Mutation {
    pub fn extract(
        map: Map<String, Value>,
        shared_props: &SharedMutationProps,
        valeurfonc: f64,
        idmutation: u64,
        id: u64,
    ) -> Result<Mutation, ()> {
        let idpar = String::from(
            map.get("idpar")
                .ok_or(())
                .map_err(|_| error!("Failed to get the value of the key 'idpar'"))?
                .as_str()
                .ok_or(())
                .map_err(|_| error!("Inconsistant value : Expected a Str"))?,
        );

        let vendu = map
            .get("parcvendue")
            .ok_or(())
            .map_err(|_| error!("Failed to get the value of the key 'parcvendue'"))?
            .as_bool()
            .ok_or(())
            .map_err(|_| error!("Inconsistant value : Expected a Boolean"))?;

        let adresses = map
            .get("adresses")
            .ok_or(())
            .map_err(|_| error!("Failed to get the value of the key 'adresses'"))?;
        let adresse = Adresse::extract(adresses)?;

        Ok(Mutation {
            idg: id,
            idpar,
            idmutation,
            shared_props: shared_props.clone(),
            adresse,
            valeur_fonciere: valeurfonc,
            vendu,
        })
    }

    fn to_record_batch(mutations: &[Mutation]) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        let mut idg_vec = Vec::new();
        let mut idpar_vec = Vec::new();
        let mut idmutation_vec = Vec::new();
        let mut vefa_vec = Vec::new();
        let mut typologie_vec = Vec::new();
        let mut datemut_vec = Vec::new();
        let mut nature_vec = Vec::new();
        let mut btq_vec = Vec::new();
        let mut voie_vec = Vec::new();
        let mut novoie_vec = Vec::new();
        let mut codvoie_vec = Vec::new();
        let mut commune_vec = Vec::new();
        let mut typvoie_vec = Vec::new();
        let mut codepostal_vec = Vec::new();
        let mut valeur_fonciere_vec = Vec::new();
        let mut vendu_vec = Vec::new();

        for mutation in mutations {
            idg_vec.push(mutation.idg);
            idpar_vec.push(mutation.idpar.clone());
            idmutation_vec.push(mutation.idmutation);

            vefa_vec.push(mutation.shared_props.vefa);
            typologie_vec.push(mutation.shared_props.typologie.clone());
            datemut_vec.push(parse_date(&mutation.shared_props.datemut));
            nature_vec.push(mutation.shared_props.nature.clone());

            btq_vec.push(mutation.adresse.btq.clone());
            voie_vec.push(mutation.adresse.voie.clone());
            novoie_vec.push(mutation.adresse.novoie.clone());
            codvoie_vec.push(mutation.adresse.codvoie.clone());
            commune_vec.push(mutation.adresse.commune.clone());
            typvoie_vec.push(mutation.adresse.typvoie.clone());
            codepostal_vec.push(mutation.adresse.codepostal.clone());

            valeur_fonciere_vec.push(mutation.valeur_fonciere);
            vendu_vec.push(mutation.vendu);
        }

        let idg_arr = Arc::new(UInt64Array::from(idg_vec)) as ArrayRef;
        let idpar_arr = Arc::new(StringArray::from(idpar_vec)) as ArrayRef;
        let idmutation_arr = Arc::new(UInt64Array::from(idmutation_vec)) as ArrayRef;
        let vefa_arr = Arc::new(BooleanArray::from(vefa_vec)) as ArrayRef;
        let typologie_arr = Arc::new(StringArray::from(typologie_vec)) as ArrayRef;
        let datemut_arr = Arc::new(Date32Array::from_iter(datemut_vec)) as ArrayRef;
        let nature_arr = Arc::new(StringArray::from(nature_vec)) as ArrayRef;
        let btq_arr = Arc::new(StringArray::from_iter(btq_vec)) as ArrayRef;
        let voie_arr = Arc::new(StringArray::from_iter(voie_vec)) as ArrayRef;
        let novoie_arr = Arc::new(UInt64Array::from_iter(novoie_vec)) as ArrayRef;
        let codvoie_arr = Arc::new(StringArray::from_iter(codvoie_vec)) as ArrayRef;
        let commune_arr = Arc::new(StringArray::from(commune_vec)) as ArrayRef;
        let typvoie_arr = Arc::new(StringArray::from_iter(typvoie_vec)) as ArrayRef;
        let codepostal_arr = Arc::new(StringArray::from(codepostal_vec)) as ArrayRef;
        let valeur_fonciere_arr = Arc::new(Float64Array::from(valeur_fonciere_vec)) as ArrayRef;
        let vendu_arr = Arc::new(BooleanArray::from(vendu_vec)) as ArrayRef;

        let schema = Schema::new(vec![
            Field::new("idg", DataType::UInt64, false),
            Field::new("idpar", DataType::Utf8, false),
            Field::new("idmutation", DataType::UInt64, false),
            Field::new("vefa", DataType::Boolean, false),
            Field::new("typologie", DataType::Utf8, true),
            Field::new("datemut", DataType::Date32, false),
            Field::new("nature", DataType::Utf8, true),
            Field::new("btq", DataType::Utf8, true),
            Field::new("voie", DataType::Utf8, true),
            Field::new("novoie", DataType::UInt64, true),
            Field::new("codvoie", DataType::Utf8, true),
            Field::new("commune", DataType::Utf8, true),
            Field::new("typvoie", DataType::Utf8, true),
            Field::new("codepostal", DataType::Utf8, true),
            Field::new("valeur_fonciere", DataType::Float64, false),
            Field::new("vendu", DataType::Boolean, true),
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                idg_arr,
                idpar_arr,
                idmutation_arr,
                vefa_arr,
                typologie_arr,
                datemut_arr,
                nature_arr,
                btq_arr,
                voie_arr,
                novoie_arr,
                codvoie_arr,
                commune_arr,
                typvoie_arr,
                codepostal_arr,
                valeur_fonciere_arr,
                vendu_arr,
            ],
        )
        .map_err(|e| e.into())
    }

    pub fn write_to_parquet(
        mutations: &[Mutation],
        path: &PathBuf,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let record_batch = Mutation::to_record_batch(mutations)?;
        let file = File::create(path)?;

        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::SNAPPY)
            .build();

        let mut writer = ArrowWriter::try_new(file, record_batch.schema(), Some(props))?;
        writer.write(&record_batch)?;
        writer.close()?;

        Ok(())
    }
}

impl SharedMutationProps {
    pub fn new(vefa: bool, typologie: String, datemut: String, nature: String) -> Self {
        Self {
            vefa,
            typologie,
            datemut,
            nature,
        }
    }
}

impl Adresse {
    fn extract(value: &Value) -> Result<Adresse, ()> {
        let adresses = value
            .as_array()
            .ok_or(())
            .map_err(|_| error!("Inconsistant value : Expected an Array<Value>"))?;
        let adresse = adresses
            .get(0)
            .ok_or(())
            .map_err(|_| error!("Empty adresses"))?
            .as_object()
            .ok_or(())
            .map_err(|_| error!("Inconsistant value : Expected a Map<String, Value>"))?;

        let commune = String::from(
            adresse
                .get("commune")
                .ok_or(())
                .map_err(|_| error!("Failed to get the value of the key 'commune'"))?
                .as_str()
                .ok_or(())
                .map_err(|_| error!("Inconsistant value : Expecteed a Str"))?,
        );

        let codepostal = String::from(
            adresse
                .get("codepostal")
                .ok_or(())
                .map_err(|_| error!("Failed to get the value of the key 'codepostal'"))?
                .as_str()
                .ok_or(())
                .map_err(|_| error!("Inconsistant value : Expecteed a Str"))?,
        );

        Ok(Adresse {
            btq: unwrap_value(adresse.get("btq")),
            voie: unwrap_value(adresse.get("voie")),
            novoie: unwrap_value(adresse.get("novoie")).and_then(|s| s.parse::<u64>().ok()),
            codvoie: unwrap_value(adresse.get("codvoie")),
            commune,
            typvoie: unwrap_value(adresse.get("typvoie")),
            codepostal,
        })
    }
}

impl Classes {
    pub fn extract(values: &Vec<Value>, id: u64) -> Result<Vec<Classes>, ()> {
        let values = values
            .into_iter()
            .map(|v| {
                if let Value::Object(map) = v {
                    match (map.get("surface"), map.get("libregroupement")) {
                        (Some(surface), Some(name)) => {
                            let surface = surface.as_f64().ok_or(())?;
                            if surface < 1f64 {
                                return Err(());
                            }
                            let name = String::from(name.as_str().ok_or(())?);
                            Ok(Classes {
                                idg: id,
                                libelle: name,
                                surface: surface,
                            })
                        }
                        _ => Err(()),
                    }
                } else {
                    Err(())
                }
            })
            .flatten()
            .collect::<Vec<Classes>>();

        Ok(values)
    }

    fn to_record_batch(classes: &[Classes]) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        let mut idg_vec = Vec::new();
        let mut libelle_vec = Vec::new();
        let mut surface_vec = Vec::new();

        for class in classes {
            idg_vec.push(class.idg);
            libelle_vec.push(class.libelle.clone());
            surface_vec.push(class.surface);
        }

        let idg_array = Arc::new(UInt64Array::from(idg_vec)) as ArrayRef;
        let libelle_array = Arc::new(StringArray::from(libelle_vec)) as ArrayRef;
        let surface_array = Arc::new(Float64Array::from(surface_vec)) as ArrayRef;

        let schema = Schema::new(vec![
            Field::new("idg", DataType::UInt64, false),
            Field::new("libelle", DataType::Utf8, false),
            Field::new("surface", DataType::Float64, false),
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![idg_array, libelle_array, surface_array],
        )
        .map_err(|e| e.into())
    }

    pub fn write_to_parquet(
        classes: &[Classes],
        path: &PathBuf,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let record_batch = Classes::to_record_batch(classes)?;
        let file = File::create(path)?;

        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::SNAPPY)
            .build();

        let mut writer = ArrowWriter::try_new(file, record_batch.schema(), Some(props))?;
        writer.write(&record_batch)?;
        writer.close()?;

        Ok(())
    }
}

fn unwrap_value(value: Option<&Value>) -> Option<String> {
    if let Some(value) = value {
        if let Some(value) = value.as_str() {
            Some(String::from(value))
        } else {
            None
        }
    } else {
        None
    }
}
