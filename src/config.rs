use clap::{App, Arg};

use crate::workload::WORKLOADS;

#[derive(Debug)]
pub struct Config {
    pub workload_name: String,
    pub mutation_checker: String,
    pub assertion: String,
    pub limit: u32,
    pub uri: String,
    pub log_path: String,
}

pub fn init_app() -> Config {
    let matches = App::new("corrupttest")
        .arg(
            Arg::new("workload")
                .short('w')
                .long("workload")
                .takes_value(true)
                .required(true)
                .possible_values(&WORKLOADS.keys().cloned().collect::<Vec<_>>()),
        )
        .arg(
            Arg::new("mutation_checker")
                .short('m')
                .long("mutation_checker")
                .takes_value(true)
                .required(true)
                .ignore_case(true)
                .possible_values(&["0", "1", "true", "false", "on", "off"]),
        )
        .arg(
            Arg::new("assertion")
                .short('a')
                .long("assertion")
                .takes_value(true)
                .required(true)
                .ignore_case(true)
                .possible_values(&["off", "fast", "strict"]),
        )
        .arg(
            Arg::new("limit")
                .short('l')
                .long("limit")
                .takes_value(true)
                .required(false)
                .default_value("0"),
        )
        .arg(
            Arg::new("uri")
                .short('u')
                .long("uri")
                .takes_value(true)
                .required(false)
                .default_value("mysql://root@127.0.0.1:4000/test"),
        )
        .arg(
            Arg::new("log_path")
                .short('o')
                .long("log_path")
                .takes_value(true)
                .required(false)
                .default_value("corrupttest.log"),
        )
        .get_matches();
    let config = Config {
        workload_name: matches
            .value_of("workload")
            .expect("must specify the workload parameter")
            .to_lowercase(),
        mutation_checker: matches.value_of("mutation_checker").unwrap().to_owned(),
        assertion: matches.value_of("assertion").unwrap().to_owned(),
        limit: matches
            .value_of("limit")
            .unwrap()
            .parse::<u32>()
            .expect("limit must be a non-negative number"),
        uri: matches.value_of("uri").unwrap().to_owned(),
        log_path: matches.value_of("log_path").unwrap().to_owned(),
    };
    config
}
