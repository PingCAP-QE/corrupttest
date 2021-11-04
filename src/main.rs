#![feature(generators, generator_trait)]

use corrupttest::{
    failpoint::{disable_failpoint, enable_failpoint},
    table::*,
    AVAILABLE_INJECTIONS, MYSQL_ADDRESS,
};
use mysql::{prelude::*, Opts, Pool};
use std::collections::HashMap;

enum Effectiveness {
    Inconsistent, // the error message contains "inconsist"-like words
    OtherError,   // other errors are reported
    NoError,      // failed to detect error
}

fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let url = format!("mysql://root@{}/test", MYSQL_ADDRESS);
    println!("{}", &url);
    let pool = Pool::new(Opts::from_url(&url)?)?;
    let mut conn = pool.get_conn().unwrap();

    let tables = TableIterator::new();
    // {table} x {workload} x {injection} -> (success, other success, failure)
    let mut results = HashMap::<(Table, String, String), Effectiveness>::new();
    let mut cnt = 0;
    // let mut success: u32 = 0; // error message containing "inconsistent"
    // let mut other_success = 0; // other errors
    // let mut failure: u32 = 0; // no error reported

    for table in tables {
        cnt += 1;
        workload_a(table, &mut conn, &mut results)?;
    }
    println!("{} tables finish", cnt);
    for &injection in AVAILABLE_INJECTIONS {
        let counts = results
            .iter()
            .filter(|(key, _)| key.2.as_str() == injection)
            .fold((0, 0, 0), |acc, (_, value)| match value {
                Effectiveness::Inconsistent => (acc.0 + 1, acc.1, acc.2),
                Effectiveness::OtherError => (acc.0, acc.1 + 1, acc.2),
                Effectiveness::NoError => (acc.0, acc.1, acc.2 + 1),
            });
        println!(
            "{}: success:{} other success:{} failure:{}",
            injection, counts.0, counts.1, counts.2
        );
    }

    // println!("{} {} {}",)
    Ok(())
}

// returns the numbers of (error containing "inconsistent", other errors reported, no error found)
fn workload_a(
    table: Table,
    conn: &mut mysql::PooledConn,
    results: &mut HashMap<(Table, String, String), Effectiveness>,
) -> Result<(), Box<dyn std::error::Error>> {
    let drop_sentence = table.drop_sentence();
    let create_sentence = table.create_sentence();
    conn.query_drop(drop_sentence)?;
    conn.query_drop(&create_sentence)?;
    println!("{}", create_sentence);

    for injection in AVAILABLE_INJECTIONS {
        enable_failpoint(
            "github.com/pingcap/tidb/table/tables/corruptMutations",
            format!("return(\"{}\")", injection),
        )?;
        let insertion = format!(
            "insert into {} values ({})",
            table.name,
            table.gen_row().join(", ")
        );
        let res = conn.query_drop(insertion);
        println!("{}", injection);
        println!("{:?}", res);

        let e = match res {
            Ok(_) => Effectiveness::NoError,
            Err(x) if x.to_string().contains("inconsist") => Effectiveness::Inconsistent,
            Err(_) => Effectiveness::OtherError,
        };
        results.insert(
            (table.clone(), "workload_a".into(), injection.to_string()),
            e,
        );
        disable_failpoint("github.com/pingcap/tidb/table/tables/corruptMutations")?;
    }

    Ok(())
}
