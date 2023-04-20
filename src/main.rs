// https://able.bio/haixuanTao/data-manipulation-pandas-vs-rust--1d70e7fc

mod colunas_a;
use colunas_a::ColunasA;

mod colunas_b;
use colunas_b::ColunasB;

//mod groupby;
// use groupby::{somar_em_paralelo, Keys, Values};

use data_manipulation::clear_terminal_screen;

use encoding_rs::{UTF_8}; // WINDOWS_1252
use encoding_rs_io::DecodeReaderBytesBuilder;
use chrono::{DateTime, Local};
use rayon::prelude::*;

use memmap2::{MmapOptions, Mmap};
use itertools::Itertools;

// https://docs.rs/csv/1.0.0/csv/tutorial/index.html
use csv::{StringRecord, Trim}; // ReaderBuilder
use serde::{Serialize, Deserialize};
use serde_aux::prelude::serde_introspect;

use std::{
    env,
    process, // process::exit(1)
    fs::File,
    error::Error,
    time::Instant,
    collections::HashMap,
};

use data_manipulation::munkres_assignments;

#[derive(Debug, Default, Serialize, Deserialize, PartialEq, PartialOrd, Clone)]
//#[serde(transparent)]
struct KeysValues {
    key: String,
    count: i64,
    //#[serde(flatten)]
    list: Vec<f64>,
    sum: f64,
    average: f64,
}

#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Clone)]
//#[serde(transparent)]
struct Colunas {
    colunas_a: ColunasA,
    colunas_b: ColunasB,
}

fn main() -> Result<(), Box<dyn Error>> {

    clear_terminal_screen();
    let time = Instant::now();

    #[allow(clippy::redundant_closure)]
    let (groupby_a, groupby_b) = rayon::join(
        || analise_file_a(),
        || analise_file_b(),
    );

    println!("Número de Chaves File A: {}", groupby_a.len());
    println!("Número de Chaves File B: {}", groupby_b.len());

    let correlations: Vec<(String, (usize, usize))> = groupby_a
    .par_iter()
    .map(|(key, tuple_a)| {
            let mut vector: Vec<(String, (usize, usize))> = Vec::new();
            match groupby_b.get(key) {
                Some(tuple_b) => {
                    let vec_assignments: Vec<usize> = munkres_assignments(tuple_a, tuple_b);
                    for (index, &num) in vec_assignments.iter().enumerate() {
                        let opt_a = tuple_a.get(index);
                        let opt_b = tuple_b.get(num);
                        match (opt_a, opt_b) {
                            (Some(a), Some(b)) => {
                                let tuple = (a.0, b.0);
                                vector.push((key.to_string(), tuple));
                            },
                            _ => continue,
                        }
                    }
                    vector
                },
                None => vector,
            }
        },
    )
    .reduce(
        Vec::new, |m1, m2| {
            m2.iter().fold(m1, |mut vector, (key, tuple)| {
                vector.push((key.to_string(), *tuple));
                vector
            })
        },
    );

    let mut count = 1;
    for (key, value) in &correlations {
        println!("key: {key} ; value: {value:?}");
        if count > 10 {
            break;
        }
        count += 1;
    }

    println!("correlations.len(): {}", correlations.len());


    /*
    let colunas: Vec<Colunas> = Vec::new();
    for (key, value) in &correlations {
        let coluna = Colunas {
            colunas_a: todo!(),
            colunas_b: todo!(),
        };
        colunas.push(coluna);
    }
    */


    let dt_local_now: DateTime<Local> = Local::now();
    println!("Data Local: {}", dt_local_now.format("%d/%m/%Y"));
    println!("Tempo de Execução Total: {:?}\n",time.elapsed());

    Ok(())
}

fn analise_file_a () -> HashMap<String, Vec<(usize, f64)>> {
   // File A
    // 1. Reading CSV

    let csv_file_a = env::args()
        .nth(1)
        .expect("supply a single path as the program argument");

    // https://github.com/rayon-rs/rayon/issues/885
    // memory_mapped_file(&csv_file_a)?;

    let (_headers_csv, vec_colunas) = match read_csv_a(&csv_file_a, '|') {
        Ok((headers, data)) => (headers, data),
        Err(e) => {
            eprintln!("Arquivo '{csv_file_a}' não encontrado!");
            eprintln!("{}", e);
            process::exit(1);
        }
    };

    // 2. Add a counter

    /*
    let mut ncolunas: Vec<(usize, &ColunasA)> = Vec::new();
    vec_colunas.iter().enumerate().for_each(|(index, colunas)| 
        ncolunas.push((index, colunas))
    );
    */

    let mut ncolunas: Vec<(usize, &ColunasA)> = vec_colunas
        .par_iter()
        .enumerate()
        .map(|(index, colunas)| {
                let mut vector: Vec<(usize, &ColunasA)> = Vec::new();
                let tuple = (index, colunas);
                vector.push(tuple);
                vector
            },
        )
        .reduce(
            Vec::new, |m1, m2| {
                m2.iter().fold(m1, |mut vector, &(count, coluna)| {
                    vector.push((count, coluna));
                    vector
                })
            },
        );

    // 3. Filtering

    ncolunas
        .retain(|&(_index, coluna)| !coluna.chave_doc.is_empty() && coluna.valor_item.is_some());
    //write_csv(&vec_colunas, "output_rust_filter.csv", ';', true)?;

    // 4. Group By

    // https://stackoverflow.com/questions/57641821/rayon-fold-into-a-hashmap
    // fold/reduce or map/reduce 
    let my_groupby: HashMap<String, Vec<(usize, f64)>> = ncolunas
        .par_iter()
        .fold(
            HashMap::new, |mut hmap, ncoluna| {
                let &(index, coluna) = ncoluna;
                let key: String = coluna.chave_doc.clone();
                let opt_value: Option<f64> = coluna.valor_item;

                match opt_value {
                    Some(value) => {
                        let tuple = (index, value);
                        hmap.entry(key).or_insert(Vec::new()).push(tuple);
                        hmap
                    },
                    None => hmap,
                }
            },
        )
        .reduce(
            HashMap::new, |m1, m2| {
                m2.iter().fold(m1, |mut hmap, (keys, value)| {
                    hmap.entry(keys.clone()).or_insert(Vec::new()).extend(value);
                    hmap
                })
            },
        );


    //let resultado: HashMap<Keys, Values> = somar_em_paralelo(&vec_colunas);
    //println!("\nNúmero de Chaves: {}", resultado.len());

    /*
    for (k, v) in &resultado {
        if my_groupby.get(&k.key).is_none() {
            println!("{k:?}: {v:?}");
        }
    }
    */

    /*
    let group_vec: Vec<KeysValues> = split_and_execute(&vec_colunas);
    let groups: Vec<KeysValues> =groupby_aggregate_total (&group_vec);
    write_csv(&groups, "output_rust_groupby.csv", ';', true)?;
    */


    // 5. Merge

    my_groupby
}

fn analise_file_b () -> HashMap<String, Vec<(usize, f64)>> {
   // File B
    // 1. Reading CSV

    // unique nfe.csv -eitwcnk > nfe_float64.csv
    // tail -n 15 nfe_float64.csv

    let csv_file_b = env::args()
        .nth(2)
        .expect("supply a single path as the program argument");

    // https://github.com/rayon-rs/rayon/issues/885
    // memory_mapped_file(&csv_file_b)?;

    let (_headers_csv, vec_colunas) = match read_csv_b(&csv_file_b, ';') {
        Ok((headers, data)) => (headers, data),
        Err(e) => {
            eprintln!("Arquivo '{csv_file_b}' não encontrado!");
            eprintln!("{}", e);
            process::exit(1);
        }
    };

    // 2. Add a counter

    let mut ncolunas: Vec<(usize, &ColunasB)> = vec_colunas
        .par_iter()
        .enumerate()
        .map(|(index, colunas)| {
                let mut vector: Vec<(usize, &ColunasB)> = Vec::new();
                let tuple = (index, colunas);
                vector.push(tuple);
                vector
            },
        )
        .reduce(
            Vec::new, |m1, m2| {
                m2.iter().fold(m1, |mut vector, &(count, coluna)| {
                    vector.push((count, coluna));
                    vector
                })
            },
        );

    // 3. Filtering

    ncolunas
        .retain(|&(_index, coluna)| {
            let valor_item: f64 = coluna.valor_item.unwrap_or(0.0);
            valor_item > 0.0 && coluna.chave_doc.is_some()
    });
    //write_csv(&vec_colunas, "output_rust_filter.csv", ';', true)?;

    /*
    for (index, ncoluna) in ncolunas.iter().enumerate() {
        println!("3 index: {index} ; ncoluna: {ncoluna:?}\n");
        if index >= 2 {
            break;
        }
    }
    */

    // 4. Group By

    // https://stackoverflow.com/questions/57641821/rayon-fold-into-a-hashmap
    // fold/reduce or map/reduce 
    let my_groupby: HashMap<String, Vec<(usize, f64)>> = ncolunas
        .par_iter()
        .fold(
            HashMap::new, |mut hmap, ncoluna| {
                let &(index, coluna) = ncoluna;
                let key: String = coluna.chave_doc.clone().unwrap().replace('\'', "");
                let opt_value: Option<f64> = coluna.valor_item;

                match opt_value {
                    Some(value) => {
                        let tuple = (index, value);
                        hmap.entry(key).or_insert(Vec::new()).push(tuple);
                        hmap
                    },
                    None => hmap,
                }
            },
        )
        .reduce(
            HashMap::new, |m1, m2| {
                m2.iter().fold(m1, |mut hmap, (keys, value)| {
                    hmap.entry(keys.clone()).or_insert(Vec::new()).extend(value);
                    hmap
                })
            },
        );

    /*
    let group_vec: Vec<KeysValues> = split_and_execute(&vec_colunas);
    let groups: Vec<KeysValues> =groupby_aggregate_total (&group_vec);
    write_csv(&groups, "output_rust_groupby.csv", ';', true)?;
    */


    // 5. Merge

    my_groupby
}

// https://stackoverflow.com/questions/46867355/is-it-possible-to-split-a-vector-into-groups-of-10-with-iterators
#[allow(dead_code)]
fn split_and_execute(vec_colunas: &[ColunasA]) -> Vec<KeysValues> {

    let cpus = num_cpus::get();
    let chunk_size: usize = vec_colunas.len()/cpus;
    println!("chunk_size: {chunk_size}");

    let vec_of_groups: Vec<Vec<KeysValues>> = vec_colunas
        .par_chunks(chunk_size)
        .map(|chunk: &[ColunasA]| {
            groupby_aggregate(chunk)
        })
        .collect();

    vec_of_groups.concat()
}

#[allow(dead_code)]
fn split_and_execute_v2(vec_colunas: &[ColunasA]) -> Vec<KeysValues> {

    let chunk_size: usize = vec_colunas.len()/8 + 1;
    println!("chunk_size: {chunk_size}");

    let workers: Vec<&[ColunasA]> = vec_colunas
        .chunks(chunk_size)
        .collect();

    // This creates the scope for the threads
    // Neste caso, a ordem de impressão é determinada!
    let (res_0, res_1, res_2, res_3, res_4, res_5, res_6, res_7) = std::thread::scope(|s| {

        let thread_0 = s.spawn(||groupby_aggregate(workers[0]));
        let thread_1 = s.spawn(||groupby_aggregate(workers[1]));
        let thread_2 = s.spawn(||groupby_aggregate(workers[2]));
        let thread_3 = s.spawn(||groupby_aggregate(workers[3]));
        let thread_4 = s.spawn(||groupby_aggregate(workers[4]));
        let thread_5 = s.spawn(||groupby_aggregate(workers[5]));
        let thread_6 = s.spawn(||groupby_aggregate(workers[6]));
        let thread_7 = s.spawn(||groupby_aggregate(workers[7]));

        // Wait for background thread to complete
        (thread_0.join(), thread_1.join(), thread_2.join(), thread_3.join(), 
         thread_4.join(), thread_5.join(), thread_6.join(), thread_7.join())
    });

    let (group0, group1, group2, group3, group4, group5, group6, group7) = match (res_0, res_1, res_2, res_3, res_4, res_5, res_6, res_7) {
        (Ok(g0), Ok(g1), Ok(g2), Ok(g3), Ok(g4), Ok(g5), Ok(g6), Ok(g7)) => (g0, g1, g2, g3, g4, g5, g6, g7),
        _ => panic!("Error thread groupby_aggregate!"),
    };

    let group_vec: Vec<KeysValues> = [group0, group1, group2, group3, group4, group5, group6, group7].concat();

    group_vec
}

#[allow(dead_code)]
fn groupby_aggregate_total (vec_colunas: &[KeysValues]) -> Vec<KeysValues> {

    let groups: Vec<KeysValues> = vec_colunas
    //.into_par_iter() // rayon: parallel iterator
    .iter()
    .sorted_unstable_by(|a, b| Ord::cmp(&a.key, &b.key))
    .group_by(|&colunas| colunas.key.clone())
    .into_iter()
    .map(|(key, group)| 
    {     
        let (count, list, sum) = group
            .into_iter()
            .fold((0, Vec::new(), 0.), |(count, list, sum), colunas|
            {
                (
                    count + colunas.count,
                    [list, colunas.list.clone()].concat(),
                    sum + colunas.sum,
                )
            },
        );

        KeysValues {
            key,
            count,
            list,
            sum,
            average: sum / (count as f64),
        }
    })
    .collect();

    groups
}

#[allow(dead_code)]
fn groupby_aggregate (vec_colunas: &[ColunasA]) -> Vec<KeysValues> {

    let groups: Vec<KeysValues> = vec_colunas
    //.into_par_iter() // rayon: parallel iterator
    .iter()
    .sorted_unstable_by(|a, b| Ord::cmp(&a.chave_doc, &b.chave_doc))
    .group_by(|&colunas| colunas.chave_doc.clone())
    .into_iter()
    .map(|(key, group)| 
    {     
        let (count, list, sum) = group
            .into_iter()
            .fold((0, Vec::new(), 0.), |(count, mut list, sum), colunas|
            {
                let value: f64 = colunas.valor_item.unwrap_or(0.);
                list.push(value);

                (
                    count + 1,
                    list,
                    sum + value,
                )
            },
        );

        KeysValues {
            key,
            count,
            list,
            sum,
            average: sum / (count as f64),
        }
    })
    .collect();

    groups
}

// https://docs.rs/csv/1.0.0/csv/tutorial/index.html
fn read_csv_a(file_path: &str, delimiter: char) -> Result<(StringRecord, Vec<ColunasA>), Box<dyn Error>> {

    let my_file = File::open(file_path)?;

    let transcoded = DecodeReaderBytesBuilder::new()
        //.encoding(Some(WINDOWS_1252))
        .encoding(Some(UTF_8))
        .build(my_file);

    let mut reader = csv::ReaderBuilder::new()
        .trim(Trim::All)
        .delimiter( delimiter as u8) // .delimiter(b'|')
        .has_headers(true)
        .from_reader(transcoded);
        //.from_path(file_path)?;

    let headers = reader.headers()?.clone();
    let colunas_vec = serde_introspect::<ColunasA>();

    println!("file_path: '{file_path}'");
    //println!("headers: {headers:?}");
    println!("colunas_vec: {colunas_vec:?}\n");

    let mut data: Vec<ColunasA> = Vec::new();

    // has_headers(true) -> don't deserialize the first row because it has headers
    for result in reader.deserialize() {

        let colunas: ColunasA = match result {
            Ok(col) => col,
            Err(err) => {
                println!("Erro em fn read_csv_a --> reader.deserialize():\n{err}");
                process::exit(1);
            }
        };

        data.push(colunas);
    }

    Result::Ok((headers, data))
}

// https://docs.rs/csv/1.0.0/csv/tutorial/index.html
fn read_csv_b(file_path: &str, delimiter: char) -> Result<(StringRecord, Vec<ColunasB>), Box<dyn Error>> {

    let my_file = File::open(file_path)?;

    let transcoded = DecodeReaderBytesBuilder::new()
        //.encoding(Some(WINDOWS_1252))
        .encoding(Some(UTF_8))
        .build(my_file);

    let mut reader = csv::ReaderBuilder::new()
        .trim(Trim::All)
        .delimiter(delimiter as u8) // .delimiter(b'|')
        .has_headers(true)
        .from_reader(transcoded);
        //.from_path(file_path)?;

    let headers = reader.headers()?.clone();
    let colunas_vec = serde_introspect::<ColunasB>();

    println!("file_path: '{file_path}'");
    //println!("headers: {headers:?}");
    println!("colunas_vec: {colunas_vec:?}\n");

    let mut data: Vec<ColunasB> = Vec::new();

    // has_headers(true) -> don't deserialize the first row because it has headers
    for result in reader.deserialize() {

        let colunas: ColunasB = match result {
            Ok(col) => col,
            Err(err) => {
                println!("Erro em fn read_csv_b --> reader.deserialize():\n{err}");
                process::exit(1);
            }
        };

        data.push(colunas);
    }

    //println!(" data.len(): {}\n", data.len());
    Result::Ok((headers, data))
}

// https://docs.rs/csv/1.0.0/csv/tutorial/index.html
// https://github.com/andrewleverette/rust_csv_examples/blob/master/src/bin/csv_write_serde.rs
#[allow(dead_code)]
fn write_csv<T>(data: &[T], file_path: &str, delimiter: char, verbose: bool) -> Result<(), Box<dyn Error>>
    where T:serde::Serialize + std::fmt::Debug
{
    //deletar_arquivo(file_path)?;

    let mut writer = csv::WriterBuilder::new()
    .delimiter(delimiter as u8) // .delimiter(b'|')
    .has_headers(true) // write the header
    .quote_style(csv::QuoteStyle::NonNumeric)
    .from_path(file_path)?;
    
    if verbose {
        for d in data {
            println!("{d:?}");
        }
        println!("data.len(): {}", data.len());
    }

    for coluna in data {
        if writer.serialize(coluna).is_err() {
            println!("\n\t Erro!");
            println!("\t Não foi possível imprimir o arquivo:");
            println!("\t '{file_path}'\n");
            break;
        }
    }

    // A CSV writer maintains an internal buffer, so it's important
    // to flush the buffer when you're done.
    writer.flush()?;

    Result::Ok(())
}

// https://github.com/rayon-rs/rayon/issues/885
/// A mapped file is a file that we can operate on as if it were in RAM. 
/// When we map it then kernel gives as a pointer to virtual memory. 
/// As we start reading RAM a page fault will occur which will tell the kernel to load another page from file.
#[allow(dead_code)]
fn memory_mapped_file(path: &str) -> Result<(), Box<dyn Error>> {

    let file = std::fs::File::open(path)?;

    let mmap: Mmap = unsafe { 
        MmapOptions::new()
        .map(&file)? 
    };

    if let Some((first, elements)) = mmap.split_first() {
        println!("first: {first}");
        println!("elements: {:?}", &elements[..10]);
    }

    //let mut iter = mmap.chunks(2);

    println!("fn memory_mapped_file() is Ok!");

    Ok(())
}