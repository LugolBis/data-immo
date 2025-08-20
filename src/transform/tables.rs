use mylog::error;
use serde::Serialize;
use serde_json::{self, Map, Value};

pub const HEADERS_MUTATION: [&str; 16] = [
    "id",
    "idpar",
    "idmutation",
    "vefa",
    "typologie",
    "datemut",
    "nature",
    "btq",
    "voie",
    "novoie",
    "codvoie",
    "commune",
    "typvoie",
    "codepostal",
    "valeur_fonciere",
    "vendu",
];

pub const HEADERS_CLASSES: [&str; 3] = ["id", "name", "surface"];

/// Represent the SQL table '***Mutation***'
#[derive(Debug, Clone, Serialize)]
pub struct Mutation {
    id: u64,
    idpar: String,
    idmutation: u64,
    #[serde(flatten)]
    shared_props: SharedMutationProps,
    #[serde(flatten)]
    adresse: Adresse,
    valeur_fonciere: f64,
    vendu: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct SharedMutationProps {
    vefa: bool,
    typologie: String,
    datemut: String,
    nature: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct Adresse {
    #[serde(skip_serializing_if = "Option::is_none")]
    btq: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    voie: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    novoie: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    codvoie: Option<String>,
    commune: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    typvoie: Option<String>,
    codepostal: String,
}

/// Represent the SQL table '***Classes***'
#[derive(Debug, Clone, Serialize)]
pub struct Classes {
    id: u64,
    name: String,
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
            .get("parcvendu")
            .ok_or(())
            .map_err(|_| error!("Failed to get the value of the key 'parcvendu'"))?
            .as_bool()
            .ok_or(())
            .map_err(|_| error!("Inconsistant value : Expected a Boolean"))?;

        let adresses = map
            .get("adresses")
            .ok_or(())
            .map_err(|_| error!("Failed to get the value of the key 'adresses'"))?;
        let adresse = Adresse::extract(adresses)?;

        Ok(Mutation {
            id,
            idpar,
            idmutation,
            shared_props: shared_props.clone(),
            adresse,
            valeur_fonciere: valeurfonc,
            vendu,
        })
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
            btq: unwrap_value(value.get("btq")),
            voie: unwrap_value(value.get("voie")),
            novoie: unwrap_value(value.get("novoie")),
            codvoie: unwrap_value(value.get("codvoie")),
            commune,
            typvoie: unwrap_value(value.get("typvoie")),
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
                                id: id,
                                name: name,
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
