#![feature(generators, generator_trait)]

use corrupttest::{
    failpoint::{disable_failpoint, enable_failpoint},
    table::*,
    AVAILABLE_INJECTIONS, MYSQL_ADDRESS,
};
use mysql::{prelude::*, Opts, Pool};
use std::{collections::HashMap, time};

enum Effectiveness {
    Inconsistent, // the error message contains "inconsist"-like words
    OtherError,   // other errors are reported
    NoError,      // failed to detect error
    Consistent,   // the injections don't affect - e.g. `admin check table` returns no error
}

fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
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
        workload_b(table, &mut conn, &mut results)?;
        println!(
            "table per second: {}",
            cnt as f32 / start.elapsed().as_secs_f32()
        );
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
    conn.query_drop(&drop_sentence)?;
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
            table.new_row().to_string()
        );
        let res = conn.query_drop(insertion);
        println!("{}", injection);
        println!("{:?}", res);

        collect_result(res, results, &table, injection, conn);
        disable_failpoint("github.com/pingcap/tidb/table/tables/corruptMutations")?;
    }
    // conn.query_drop(&drop_sentence)?;

    Ok(())
}

// tow insertions that test missing index
fn workload_b(
    table: Table,
    conn: &mut mysql::PooledConn,
    results: &mut HashMap<(Table, String, String), Effectiveness>,
) -> Result<(), Box<dyn std::error::Error>> {
    let drop_sentence = table.drop_sentence();
    let create_sentence = table.create_sentence();
    conn.query_drop(&drop_sentence)?;
    conn.query_drop(&create_sentence)?;
    println!("{}", create_sentence);

    for injection in AVAILABLE_INJECTIONS {
        println!("{} ready to go!", injection);
        // let mut input_string = String::new();
        // stdin()
        //     .read_line(&mut input_string)
        //     .ok()
        //     .expect("Failed to read line");

        // NOTE: "1*" here, otherwise an index mutation is missing for each row insertion, thus cannot be detected.
        enable_failpoint(
            "github.com/pingcap/tidb/table/tables/corruptMutations",
            format!("1*return(\"{}\")", injection),
        )?;
        conn.query_drop("BEGIN OPTIMISTIC")?;
        let row = table.new_row();
        let insertion_1 = format!("INSERT INTO {} VALUES ({})", table.name, row.to_string());
        let insertion_2 = format!(
            "INSERT INTO {} VALUES ({})",
            table.name,
            row.next().to_string()
        );
        println!("execute: {}", insertion_1);
        let res = conn
            .query_drop(insertion_1)
            .and_then(|_| {
                println!("execute: {}", insertion_2);
                conn.query_drop(insertion_2)
            })
            .and_then(|_| {
                println!("execute: commit");
                conn.query_drop("commit")
            });
        if res.is_err() {
            println!("execute: rollback");
            conn.query_drop("ROLLBACK")?;
        }
        println!("{:?}", res);

        collect_result(res, results, &table, injection, conn);
        disable_failpoint("github.com/pingcap/tidb/table/tables/corruptMutations")?;
    }
    // conn.query_drop(&drop_sentence)?;

    Ok(())
}

fn collect_result(
    res: Result<(), mysql::Error>,
    results: &mut HashMap<(Table, String, String), Effectiveness>,
    table: &Table,
    injection: &str,
    conn: &mut mysql::PooledConn,
) {
    let e = match res {
        Ok(_) => match conn.query_drop(format!("admin check table {}", table.name)) {
            Ok(_) => Effectiveness::Consistent,
            Err(_) => Effectiveness::NoError,
        },
        Err(x) if x.to_string().contains("inconsist") => Effectiveness::Inconsistent,
        Err(_) => Effectiveness::OtherError,
    };
    results.insert(
        (table.clone(), "workload_a".into(), injection.to_string()),
        e,
    );
}
