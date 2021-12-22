use crate::config::Config;
use crate::Result;
use crate::CREATE_TABLE_DURAION_MS;
use crate::{
    failpoint::{disable_failpoint, enable_failpoint},
    table::Table,
    Effectiveness, AVAILABLE_INJECTIONS,
};
use async_trait::async_trait;
use slog::{info, Logger};
use sqlx::MySqlConnection;
use sqlx::{query, Executor, MySql, Pool};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::time::Instant;

macro_rules! send {
    ($conn:ident, $q: expr) => {
        $conn.execute($q).await
    };
}

// a helper function that collects the results of an execution of a workload
async fn collect_result(
    res: std::result::Result<(), sqlx::Error>,
    results: &mut HashMap<(Table, String, String), Effectiveness>,
    table: &Table,
    injection: &str,
    pool: Arc<Pool<MySql>>,
) {
    let e = match res {
        Ok(_) => match send!(
            pool,
            query(format!("admin check table {}", table.name).as_str())
        ) {
            Ok(_) => Effectiveness::Consistent,
            Err(_) => Effectiveness::Failure,
        },
        Err(x)
            if x.to_string().to_lowercase().contains("inconsist")
                || x.to_string().to_lowercase().contains("assertion") =>
        {
            Effectiveness::Success
        }
        Err(_) => Effectiveness::OtherError,
    };
    results.insert(
        (table.clone(), "workload_a".into(), injection.to_string()),
        e,
    );
}

// a helper function
// system variables should be set in the same connection that will run the workload.
async fn enable_featuers(conn: &mut MySqlConnection, config: &Config) -> Result<()> {
    send!(
        conn,
        query(
            format!(
                "set @@tidb_enable_mutation_checker = {}",
                config.mutation_checker
            )
            .as_str(),
        )
    )?;

    send!(
        conn,
        query(format!("set @@tidb_txn_assertion_level = {}", config.assertion).as_str())
    )?;
    Ok(())
}

#[async_trait]
pub trait Workload {
    async fn execute(
        &self,
        log: Logger,
        config: &Config,
        table: Table,
        client: &reqwest::Client,
        pool: Arc<Pool<MySql>>,
        results: &mut HashMap<(Table, String, String), Effectiveness>,
    ) -> Result<()>;
}

pub fn find_workload(name: &str) -> Box<dyn Workload> {
    match name {
        "single" => Box::new(SingleInsertion),
        "double" => Box::new(DoubleInsertion),
        "t2" => Box::new(T2),
        _ => unimplemented!(),
    }
}

// a single insert
struct SingleInsertion;
#[async_trait]
impl Workload for SingleInsertion {
    async fn execute(
        &self,
        log: Logger,
        config: &Config,
        table: Table,
        client: &reqwest::Client,
        pool: Arc<Pool<MySql>>,
        results: &mut HashMap<(Table, String, String), Effectiveness>,
    ) -> Result<()> {
        let mut conn = pool.acquire().await?;
        enable_featuers(&mut conn, config).await?;
        let drop_statement = table.drop_statement();
        let create_statement = table.create_statement();
        info!(log, "{}", create_statement);
        send!(conn, query(drop_statement.as_str()))?;
        send!(conn, query(create_statement.as_str()))?;

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
            let res = send!(conn, query(insertion.as_str())).map(|_| ());
            info!(log, "{}; {}", injection, insertion);
            info!(log, "{:?}", res);

            collect_result(res, results, &table, injection, pool.clone()).await;
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
struct DoubleInsertion;
#[async_trait]
impl Workload for DoubleInsertion {
    async fn execute(
        &self,
        log: Logger,
        config: &Config,
        table: Table,
        client: &reqwest::Client,
        pool: Arc<Pool<MySql>>,
        results: &mut HashMap<(Table, String, String), Effectiveness>,
    ) -> Result<()> {
        let mut conn = pool.acquire().await?;
        enable_featuers(&mut conn, config).await?;
        let drop_statement = table.drop_statement();
        let create_statement = table.create_statement();
        send!(conn, drop_statement.as_str())?;
        send!(conn, create_statement.as_str())?;
        info!(log, "{}", create_statement);

        for injection in AVAILABLE_INJECTIONS {
            info!(log, "{} ready to go!", injection);

            // NOTE: "1*" here, otherwise an index mutation is missing for each row insertion, thus cannot be detected.
            enable_failpoint(
                client,
                "github.com/pingcap/tidb/table/tables/corruptMutations",
                format!("1*return(\"{}\")", injection),
            )
            .await?;
            send!(conn, "BEGIN OPTIMISTIC")?;
            let row = table.new_row();
            let insertion_1 = format!("INSERT INTO {} VALUES ({})", table.name, row.to_string());
            let insertion_2 = format!(
                "INSERT INTO {} VALUES ({})",
                table.name,
                row.next().to_string()
            );

            let res = async {
                send!(conn, query(insertion_1.as_str()))?;
                send!(conn, query(insertion_2.as_str()))?;
                Ok(())
            }
            .await;

            if res.is_err() {
                send!(conn, "ROLLBACK")?;
            }
            info!(log, "{:?}", res);

            collect_result(res, results, &table, injection, pool.clone()).await;
            disable_failpoint(
                client,
                "github.com/pingcap/tidb/table/tables/corruptMutations",
            )
            .await?;
        }

        Ok(())
    }
}

// 2 txns. The first writes corrupted data and the second reads it. Check if Assertion can detect it.
struct T2;
#[async_trait]
impl Workload for T2 {
    async fn execute(
        &self,
        log: Logger,
        config: &Config,
        table: Table,
        client: &reqwest::Client,
        pool: Arc<Pool<MySql>>,
        results: &mut HashMap<(Table, String, String), Effectiveness>,
    ) -> Result<()> {
        let mut conn = pool.acquire().await?;
        enable_featuers(&mut conn, config).await?;
        let drop_statement = table.drop_statement();
        let create_statement = table.create_statement();
        let start = Instant::now();
        send!(conn, query(drop_statement.as_str())).expect("don't let drop statement fail");
        send!(conn, query(create_statement.as_str())).expect("don't let create statement fail");
        let duration = start.elapsed();
        CREATE_TABLE_DURAION_MS.fetch_add(duration.as_millis() as u64, Ordering::SeqCst);
        info!(log, "{}", create_statement);

        for injection in AVAILABLE_INJECTIONS {
            info!(log, "{} ready to go!", injection);

            // NOTE: "1*" here, otherwise an index mutation is missing for each row insertion, thus cannot be detected.
            enable_failpoint(
                client,
                "github.com/pingcap/tidb/table/tables/corruptMutations",
                format!("1*return(\"{}\")", injection),
            )
            .await?;
            send!(conn, "begin optimistic")?;
            let row = table.new_row();
            let insertion = format!("INSERT INTO {} VALUES ({})", table.name, row.to_string());
            let deletion = format!(
                "DELETE FROM {} WHERE {} = {}",
                table.name,
                table.cols[0].name,
                row.cols[0].to_string()
            );
            let update = format!(
                "UPDATE {} SET {} = {} WHERE {} = {}",
                table.name,
                table.cols[0].name,
                row.cols[0].next().to_string(),
                table.cols[1].name,
                row.cols[1].to_string()
            );

            let res = async {
                send!(conn, query(insertion.as_str()))?;
                send!(conn, "commit")?;
                send!(conn, "begin optimistic")?;
                send!(conn, query(deletion.as_str()))?;
                send!(conn, query(update.as_str()))?;
                send!(conn, "commit")?;
                Ok(())
            }
            .await;

            if res.is_err() {
                send!(conn, "rollback")?;
            }
            info!(log, "workload finished"; "result" => ?res);

            collect_result(res, results, &table, injection, pool.clone()).await;
            disable_failpoint(
                client,
                "github.com/pingcap/tidb/table/tables/corruptMutations",
            )
            .await?;
        }

        Ok(())
    }
}
