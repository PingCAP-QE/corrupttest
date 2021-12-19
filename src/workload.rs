use crate::config::Config;
use crate::{
    failpoint::{disable_failpoint, enable_failpoint},
    table::Table,
    Effectiveness, AVAILABLE_INJECTIONS,
};
use async_trait::async_trait;
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

// a helper function
fn enable_featuers(
    conn: &mut mysql::PooledConn,
    config: &Config,
) -> Result<(), Box<dyn std::error::Error>> {
    conn.query_drop(format!(
        "set @@tidb_enable_mutation_checker = {}",
        config.mutation_checker
    ))?;
    conn.query_drop(format!(
        "set @@tidb_txn_assertion_level = {}",
        config.assertion
    ))?;
    Ok(())
}

#[async_trait]
pub trait Workload {
    async fn execute(
        &self,
        config: &Config,
        table: Table,
        client: &reqwest::Client,
        conn: &mut mysql::PooledConn,
        results: &mut HashMap<(Table, String, String), Effectiveness>,
    ) -> Result<(), Box<dyn std::error::Error>>;
}

pub fn find_workload(name: &str) -> Box<dyn Workload> {
    match name {
        "single" => Box::new(SingleInsertion),
        "double" => Box::new(DoubleInsertion),
        _ => unimplemented!(),
    }
}

// a single insert
#[derive(Clone)]
struct SingleInsertion;
#[async_trait]
impl Workload for SingleInsertion {
    async fn execute(
        &self,
        config: &Config,
        table: Table,
        client: &reqwest::Client,
        conn: &mut mysql::PooledConn,
        results: &mut HashMap<(Table, String, String), Effectiveness>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        enable_featuers(conn, config)?;
        let drop_statement = table.drop_statement();
        let create_statement = table.create_statement();
        conn.query_drop(&drop_statement)?;
        conn.query_drop(&create_statement)?;
        println!("{}", create_statement);

        for injection in AVAILABLE_INJECTIONS {
            enable_failpoint(
                client,
                "github.com/pingcap/tidb/table/tables/corruptMutations",
                format!("return(\"{}\")", injection),
            )
            .await?;
            let insertion = format!(
                "insert into {} values ({})",
                table.name,
                table.new_row().to_string()
            );
            let res = conn.query_drop(insertion);
            println!("{}", injection);
            println!("{:?}", res);

            collect_result(res, results, &table, injection, conn);
            disable_failpoint(
                client,
                "github.com/pingcap/tidb/table/tables/corruptMutations",
            )
            .await?;
        }

        Ok(())
    }
}

// two insertions that test missing index
#[derive(Clone)]
struct DoubleInsertion;
#[async_trait]
impl Workload for DoubleInsertion {
    async fn execute(
        &self,
        config: &Config,
        table: Table,
        client: &reqwest::Client,
        conn: &mut mysql::PooledConn,
        results: &mut HashMap<(Table, String, String), Effectiveness>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        enable_featuers(conn, config)?;
        let drop_statement = table.drop_statement();
        let create_statement = table.create_statement();
        conn.query_drop(&drop_statement)?;
        conn.query_drop(&create_statement)?;
        println!("{}", create_statement);

        for injection in AVAILABLE_INJECTIONS {
            println!("{} ready to go!", injection);

            // NOTE: "1*" here, otherwise an index mutation is missing for each row insertion, thus cannot be detected.
            enable_failpoint(
                client,
                "github.com/pingcap/tidb/table/tables/corruptMutations",
                format!("1*return(\"{}\")", injection),
            )
            .await?;
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
            disable_failpoint(
                client,
                "github.com/pingcap/tidb/table/tables/corruptMutations",
            )
            .await?;
        }

        Ok(())
    }
}
