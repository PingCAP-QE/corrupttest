use clap::{App, Arg};

#[derive(Debug)]
pub struct Config {
    pub workload_name: String,
    pub mutation_checker: String,
    pub assertion: String,
    pub limit: Option<u32>,
}

pub fn init_app() -> Config {
    let matches = App::new("corrupttest")
        .arg(
            Arg::with_name("workload")
                .short("w")
                .long("workload")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("mutation_checker")
                .short("m")
                .long("mutation_checker")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("assertion")
                .short("a")
                .long("assertion")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("limit")
                .short("l")
                .long("limit")
                .takes_value(true)
                .required(false),
        )
        .get_matches();
    let config = Config {
        workload_name: matches
            .value_of("workload")
            .expect("must specify the workload parameter")
            .to_lowercase(),
        mutation_checker: matches.value_of("mutation_checker").unwrap().to_owned(),
        assertion: matches.value_of("assertion").unwrap().to_owned(),
        limit: matches.value_of("limit").map(|l| {
            l.parse::<u32>()
                .expect("limit must be a non-negative number")
        }),
    };
    config
}
