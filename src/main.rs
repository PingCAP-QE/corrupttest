#![feature(generators, generator_trait)]

use clap::{App, Arg, SubCommand};
use corrupttest::{
    table::*,
    workload::{self, register_workloads},
    Effectiveness, AVAILABLE_INJECTIONS, MYSQL_ADDRESS,
};
use mysql::{prelude::*, Opts, Pool};
use std::{collections::HashMap, time};

fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let workload = init_app();

    let url = format!("mysql://root@{}/test", MYSQL_ADDRESS);
    println!("{}", &url);
    let pool = Pool::new(Opts::from_url(&url)?)?;
    let mut conn = pool.get_conn().unwrap();
    conn.query_drop("SET GLOBAL tidb_txn_mode = 'optimistic';")?;
    conn.query_drop("SET tidb_txn_mode = 'optimistic';")?;
    let tables = TableIterator::new();
    // {table} x {workload} x {injection} -> (success, other success, failure)
    let mut results = HashMap::<(Table, String, String), Effectiveness>::new();
    let mut cnt = 0;
    let start = time::Instant::now();
    for table in tables {
        cnt += 1;
        workload(table, &mut conn, &mut results)?;
        println!(
            "table per second: {}",
            cnt as f32 / start.elapsed().as_secs_f32()
        );
        if cnt == 100 {
            break;
        }
    }
    println!("{} tables finish", cnt);
    for &injection in AVAILABLE_INJECTIONS {
        let counts = results
            .iter()
            .filter(|(key, _)| key.2.as_str() == injection)
            .fold((0, 0, 0, 0), |acc, (_, value)| match value {
                Effectiveness::Inconsistent => (acc.0 + 1, acc.1, acc.2, acc.3),
                Effectiveness::OtherError => (acc.0, acc.1 + 1, acc.2, acc.3),
                Effectiveness::NoError => (acc.0, acc.1, acc.2 + 1, acc.3),
                Effectiveness::Consistent => (acc.0, acc.1, acc.2, acc.3 + 1),
            });
        println!(
            "{}: success:{} other success:{} failure:{}, consistent:{}",
            injection, counts.0, counts.1, counts.2, counts.3
        );
    }

    Ok(())
}

fn init_app() -> workload::Workload {
    let matches = App::new("corrupttest")
        .arg(
            Arg::with_name("workload")
                .short("w")
                .long("workload")
                .takes_value(true)
                .required(true),
        )
        .get_matches();
    let workload_name = matches
        .value_of("workload")
        .expect("must specify the workload parameter")
        .to_lowercase();
    let workloads = register_workloads();
    let workload = workloads.get(&workload_name).expect("workload not found");
    workload.clone()
}
