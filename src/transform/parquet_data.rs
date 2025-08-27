use arrow::{
    array::{ArrayRef, BooleanArray, Date32Array, Float64Array, StringArray, UInt64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::fs::File;
use std::sync::Arc;
use std::{error::Error, path::PathBuf};

use crate::transform::utils::parse_date;

use super::tables::{Classes, Mutation};

pub trait ParquetData {
    fn to_arrays(data: &[Self]) -> Vec<ArrayRef>
    where
        Self: Sized;

    fn get_schema() -> Schema;

    fn write_to_parquet(data: &[Self], path: &PathBuf) -> Result<(), Box<dyn Error>>
    where
        Self: Sized,
    {
        let arrays = Self::to_arrays(data);
        let schema = Arc::new(Self::get_schema());
        let record_batch = RecordBatch::try_new(schema.clone(), arrays)?;

        let file = File::create(path)?;
        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::SNAPPY)
            .build();

        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(&record_batch)?;
        writer.close()?;

        Ok(())
    }
}

impl ParquetData for Classes {
    fn to_arrays(data: &[Self]) -> Vec<ArrayRef> {
        let mut idg_vec = Vec::new();
        let mut libelle_vec = Vec::new();
        let mut surface_vec = Vec::new();

        for class in data {
            idg_vec.push(Some(class.idg));
            libelle_vec.push(Some(class.libelle.clone()));
            surface_vec.push(Some(class.surface));
        }

        vec![
            Arc::new(UInt64Array::from_iter(idg_vec)),
            Arc::new(StringArray::from_iter(libelle_vec)),
            Arc::new(Float64Array::from_iter(surface_vec)),
        ]
    }

    fn get_schema() -> Schema {
        Schema::new(vec![
            Field::new("idg", DataType::UInt64, false),
            Field::new("libelle", DataType::Utf8, false),
            Field::new("surface", DataType::Float64, false),
        ])
    }
}

impl ParquetData for Mutation {
    fn to_arrays(data: &[Self]) -> Vec<ArrayRef> {
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

        for mutation in data {
            idg_vec.push(mutation.idg);
            idpar_vec.push(mutation.idpar.clone());
            idmutation_vec.push(mutation.idmutation);

            vefa_vec.push(mutation.shared_props.vefa);
            typologie_vec.push(mutation.shared_props.typologie.clone());
            datemut_vec.push(parse_date(&mutation.shared_props.datemut));
            nature_vec.push(mutation.shared_props.nature.clone());

            btq_vec.push(mutation.adresse.btq.clone());
            voie_vec.push(mutation.adresse.voie.clone());
            novoie_vec.push(mutation.adresse.novoie);
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
        ]
    }

    fn get_schema() -> Schema {
        Schema::new(vec![
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
        ])
    }
}
