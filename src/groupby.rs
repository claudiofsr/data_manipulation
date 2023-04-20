// https://stackoverflow.com/questions/57641821/rayon-fold-into-a-hashmap
// https://github.com/xacrimon/dashmap

use crate::colunas_a::ColunasA;
use rayon::prelude::*;
use serde::{Serialize, Deserialize};
use dashmap::DashMap;
use std::{
    ops::Add,
    sync::{Arc, Mutex},
    collections::HashMap,
};

#[derive(Debug, Default, Serialize, Deserialize, PartialEq, PartialOrd, Clone, Hash, Eq)]
//#[serde(transparent)]
pub struct Keys {
    pub key: String,
}

#[derive(Debug, Default, Serialize, Deserialize, PartialEq, PartialOrd, Clone)]
pub struct Values {
    pub count: i64,
    pub list: Vec<f64>,
    pub sum: f64,
    pub average: f64,
}

// https://practice.rs/generics-traits/advanced-traits.html#default-generic-type-parameters
// https://stackoverflow.com/questions/73663781/how-to-implement-sum-of-optiont-variables
impl Add for Values {
    type Output = Self;
    fn add(self, other: Self) -> Self {

        let new_count = self.count + other.count;
        let new_sum   = self.sum   + other.sum;

        Self {
            count:   new_count,
            list:    [self.list, other.list].concat(),
            sum:     new_sum,
            average: new_sum / ( new_count as f64 ),
        }
    }
}

#[allow(dead_code)]
pub fn somar_em_paralelo_v2(linhas: &[ColunasA]) -> HashMap<Keys, Values> {

    // https://docs.rs/dashmap/latest/dashmap/
    let my_dashmap: DashMap<Keys, Values> = DashMap::new();

    linhas
        .par_iter() // rayon: parallel iterator
        .for_each(|linha| {

            let (chaves, valores) = obter_chaves_valores(linha);

            // impl Add for Values: Soma de Values
            let valores_soma: Values = match my_dashmap.get(&chaves) {
                Some(valor_anterior) => valores + valor_anterior.clone(),
                None                 => valores,
            };

            my_dashmap.insert(chaves, valores_soma);
        });

    let mut my_hashmap: HashMap<Keys, Values> = HashMap::new();

    for (k, v) in my_dashmap {
        my_hashmap.insert(k, v);
    }

    my_hashmap
}

#[allow(dead_code)]
pub fn somar_em_paralelo(linhas: &[ColunasA]) -> HashMap<Keys, Values> {

    let database: Arc<Mutex<HashMap<Keys, Values>>>= Arc::new(Mutex::new(HashMap::new()));

    let cpus = num_cpus::get();
    let chunk_size: usize = linhas.len()/cpus;

    linhas
        .par_chunks(chunk_size) // rayon: parallel iterator
        .for_each(|nlinhas| {
            let resultado_parcial: HashMap<Keys, Values> = realizar_somas_parciais(nlinhas);
            if !resultado_parcial.is_empty() {
                for (chaves, valores) in resultado_parcial {
                    let valores_soma = match database.lock().unwrap().get(&chaves) {
                        Some(valor_anterior) => valores + valor_anterior.clone(),
                        None                 => valores,
                    };
                    database.lock().unwrap().insert(chaves, valores_soma);
                }
            }
        });

    // Retornar HashMap dentro do Arc<Mutex<HashMap()>>
    // https://stackoverflow.com/questions/51335679/where-is-a-mutexguard-if-i-never-assign-it-to-a-variable
    // https://stackoverflow.com/questions/67211884/how-do-i-remove-mutexguard-around-a-value
    let mutex = Arc::try_unwrap(database).unwrap();
    mutex.into_inner().unwrap()
}

fn realizar_somas_parciais(linhas: &[ColunasA]) -> HashMap<Keys, Values> {

    let mut resultado: HashMap<Keys, Values> = HashMap::new();

    // realizar somatórios
    for linha in linhas {

        let (chaves, valores) = obter_chaves_valores(linha);

        // impl Add for Values: Soma de Values
        let valores_soma: Values = match resultado.get(&chaves) {
            Some(valor_anterior) => valores + valor_anterior.clone(),
            None                 => valores,
        };

        resultado.insert(chaves, valores_soma);
    };

    resultado
}

fn obter_chaves_valores(linha: &ColunasA) -> (Keys, Values) {

    let chaves = Keys {
        key: linha.chave_doc.clone(),
    };

    let valor_item: f64= linha.valor_item.unwrap_or(0.0);

    let valores = Values {
        count:   1,                // valor inicial
        list:    vec![valor_item], // valor inicial
        sum:     valor_item,       // valor inicial
        average: valor_item,       // valor inicial
    };

    (chaves, valores)
}