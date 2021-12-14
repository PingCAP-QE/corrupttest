use crate::{
    failpoint::{disable_failpoint, enable_failpoint},
    table::Table,
    Effectiveness, AVAILABLE_INJECTIONS,
};
use mysql::prelude::*;
use std::collections::HashMap;

// a helper function that collects the results of an execution of a workload
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

pub type Workload = fn(
    Table,
    &mut mysql::PooledConn,
    &mut HashMap<(Table, String, String), Effectiveness>,
) -> Result<(), Box<dyn std::error::Error>>;

pub fn register_workloads() -> HashMap<String, Workload> {
    let mut map: HashMap<String, Workload> = HashMap::new();
    map.insert("single".to_owned(), single_insertion);
    map.insert("double".to_owned(), double_insertions);
    map
}

// a single insert
pub fn single_insertion(
    table: Table,
    conn: &mut mysql::PooledConn,
    results: &mut HashMap<(Table, String, String), Effectiveness>,
) -> Result<(), Box<dyn std::error::Error>> {
    let drop_statement = table.drop_statement();
    let create_statement = table.create_statement();
    conn.query_drop(&drop_statement)?;
    conn.query_drop(&create_statement)?;
    println!("{}", create_statement);

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

    Ok(())
}

// two insertions that test missing index
pub fn double_insertions(
    table: Table,
    conn: &mut mysql::PooledConn,
    results: &mut HashMap<(Table, String, String), Effectiveness>,
) -> Result<(), Box<dyn std::error::Error>> {
    let drop_statement = table.drop_statement();
    let create_statement = table.create_statement();
    conn.query_drop(&drop_statement)?;
    conn.query_drop(&create_statement)?;
    println!("{}", create_statement);

    for injection in AVAILABLE_INJECTIONS {
        println!("{} ready to go!", injection);

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

    Ok(())
}
