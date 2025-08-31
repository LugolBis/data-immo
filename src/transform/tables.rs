use mylog::error;
use serde_json::{self, Map, Value};

/// Represent the SQL table '***Mutation***'
#[derive(Debug, Clone)]
pub struct Mutation {
    pub idg: u64,
    pub idpar: String,
    pub idmutation: u64,
    pub shared_props: SharedMutationProps,
    pub adresse: Adresse,
    pub valeur_fonciere: f64,
    pub vendu: bool,
}

#[derive(Debug, Clone)]
pub struct SharedMutationProps {
    pub vefa: bool,
    pub typologie: String,
    pub datemut: String,
    pub nature: String,
}

#[derive(Debug, Clone)]
pub struct Adresse {
    pub btq: Option<String>,
    pub voie: Option<String>,
    pub novoie: Option<String>,
    pub codvoie: Option<String>,
    pub commune: Option<String>,
    pub typvoie: Option<String>,
    pub codepostal: Option<String>,
}

/// Represent the SQL table '***Classes***'
#[derive(Debug, Clone)]
pub struct Classes {
    pub idg: u64,
    pub libelle: String,
    pub surface: f64,
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
            .first()
            .ok_or(())
            .map_err(|_| error!("Empty adresses"))?
            .as_object()
            .ok_or(())
            .map_err(|_| error!("Inconsistant value : Expected a Map<String, Value>"))?;

        Ok(Adresse {
            btq: unwrap_value(adresse.get("btq")),
            voie: unwrap_value(adresse.get("voie")),
            novoie: unwrap_value(adresse.get("novoie")),
            codvoie: unwrap_value(adresse.get("codvoie")),
            commune: unwrap_value(adresse.get("commune")),
            typvoie: unwrap_value(adresse.get("typvoie")),
            codepostal: unwrap_value(adresse.get("codepostal")),
        })
    }
}

impl Classes {
    pub fn extract(values: &[Value], id: u64) -> Result<Vec<Classes>, ()> {
        let values = values
            .iter()
            .flat_map(|v| {
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
                                surface,
                            })
                        }
                        _ => Err(()),
                    }
                } else {
                    Err(())
                }
            })
            .collect::<Vec<Classes>>();

        Ok(values)
    }
}

fn unwrap_value(value: Option<&Value>) -> Option<String> {
    if let Some(value) = value {
        value.as_str().map(String::from)
    } else {
        None
    }
}
