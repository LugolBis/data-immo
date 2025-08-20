use mylog::error;
use serde::Serialize;
use serde_json::{self, Map, Value};

/// Represent the SQL table '***Mutation***'
#[derive(Debug, Clone)]
pub struct Mutation {
    id: u64,
    idpar: String,
    idmutation: u64,
    shared_props: SharedMutationProps,
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

impl Serialize for Mutation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("Mutation", 2)?;
        state.serialize_field("id", &self.id)?;
        state.serialize_field("idpar", &self.idpar)?;
        state.serialize_field("idmutation", &self.idmutation)?;
        // SharedMutationProps part
        state.serialize_field("vefa", &self.shared_props.vefa)?;
        state.serialize_field("typologie", &self.shared_props.typologie)?;
        state.serialize_field("datemut", &self.shared_props.datemut)?;
        state.serialize_field("nature", &self.shared_props.nature)?;
        // Adresse part
        state.serialize_field("btq", &self.adresse.btq)?;
        state.serialize_field("voie", &self.adresse.voie)?;
        state.serialize_field("novoie", &self.adresse.novoie)?;
        state.serialize_field("codvoie", &self.adresse.codvoie)?;
        state.serialize_field("commune", &self.adresse.commune)?;
        state.serialize_field("typvoie", &self.adresse.typvoie)?;
        state.serialize_field("codepostal", &self.adresse.codepostal)?;

        state.serialize_field("valeur_fonciere", &self.valeur_fonciere)?;
        state.serialize_field("vendu", &self.vendu)?;

        state.end()
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
            novoie: unwrap_value(adresse.get("novoie")),
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
