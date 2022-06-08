use crate::config::Config;
use crate::Result;
use crate::CREATE_TABLE_DURAION_MS;
use crate::{
    failpoint::{disable_failpoint, enable_failpoint},
    table::Table,
    Effectiveness, AVAILABLE_INJECTIONS,
};
use async_trait::async_trait;
use lazy_static::lazy_static;
use slog::{info, Logger};
use sqlx::MySqlConnection;
use sqlx::{query, Executor, MySql, Pool};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::time::Instant;

lazy_static! {
    pub static ref WORKLOADS: HashMap<&'static str, Arc<dyn Workload + Sync + Send>> = {
        let mut m: HashMap<&'static str, Arc<dyn Workload + Sync + Send>> = HashMap::new();
        m.insert("single", Arc::new(SingleInsertion));
        m.insert("double", Arc::new(DoubleInsertion));
        m.insert("t2", Arc::new(T2));
        m.insert("t3", Arc::new(T3));
        m.insert("t4", Arc::new(T4));
        m
    };
}

macro_rules! send {
    ($conn:ident, $q: expr) => {
        $conn.execute($q).await
    };
    ($log:ident, $conn:ident, $q: expr) => {
        {
            info!($log, "executing"; "query" => $q.to_string());
            $conn.execute($q).await
        }
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
        Ok(_) => match send!(pool, format!("admin check table {}", table.name).as_str()) {
            Ok(_) => Effectiveness::Consistent,
            Err(_) => Effectiveness::Failure,
        },
        Err(x)
            if x.to_string().to_lowercase().contains("inconsist")
                || x.to_string().to_lowercase().contains("assertion") =>
        {
            // note: if we run `admin check table` here and get no error, it doesn't mean it's a misreport.
            // It's possible that the txn containing corrupted data is aborted because inconsistency is detected.
            // Then admin check table will not report inconsistency because corrupted data is not written to TiKV.
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
        format!("set @@tidb_txn_assertion_level = {}", config.assertion).as_str()
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

pub fn find_workload(name: &str) -> Arc<dyn Workload> {
    WORKLOADS.get(name).unwrap().clone()
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

        for injection in AVAILABLE_INJECTIONS {
            send!(log, conn, drop_statement.as_str()).expect("don't let drop statement fail");
            send!(log, conn, create_statement.as_str()).expect("don't let create statement fail");
            enable_failpoint(
                &log,
                client,
                config.status_addr.clone(),
                "github.com/pingcap/tidb/table/tables/corruptMutations",
                format!("return(\"{}\")", injection),
            )
            .await?;
            let insertion = format!(
                "insert into {} values ({})",
                table.name,
                table.new_row().to_string()
            );
            let res = send!(log, conn, insertion.as_str()).map(|_| ());
            info!(log, "workload finished"; "result" => ?res);

            collect_result(res, results, &table, injection, pool.clone()).await;
            disable_failpoint(
                &log,
                client,
                config.status_addr.clone(),
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

        for injection in AVAILABLE_INJECTIONS {
            send!(log, conn, drop_statement.as_str()).expect("don't let drop statement fail");
            send!(log, conn, create_statement.as_str()).expect("don't let create statement fail");
            info!(log, "{} ready to go!", injection);

            // NOTE: "1*" here, otherwise an index mutation is missing for each row insertion, thus cannot be detected.
            enable_failpoint(
                &log,
                client,
                config.status_addr.clone(),
                "github.com/pingcap/tidb/table/tables/corruptMutations",
                format!("1*return(\"{}\")", injection),
            )
            .await?;
            send!(log, conn, "BEGIN OPTIMISTIC")?;
            let row = table.new_row();
            let insertion_1 = format!("INSERT INTO {} VALUES ({})", table.name, row.to_string());
            let insertion_2 = format!(
                "INSERT INTO {} VALUES ({})",
                table.name,
                row.next().to_string()
            );

            let res = async {
                send!(log, conn, insertion_1.as_str())?;
                send!(log, conn, insertion_2.as_str())?;
                Ok(())
            }
            .await;

            if res.is_err() {
                send!(log, conn, "ROLLBACK")?;
            }
            info!(log, "workload finished"; "result" => ?res);

            collect_result(res, results, &table, injection, pool.clone()).await;
            disable_failpoint(
                &log,
                client,
                config.status_addr.clone(),
                "github.com/pingcap/tidb/table/tables/corruptMutations",
            )
            .await?;
        }

        Ok(())
    }
}

// 2 txns. The first writes corrupted data and the second updates it. Check if Assertion can detect it.
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
        let duration = start.elapsed();
        CREATE_TABLE_DURAION_MS.fetch_add(duration.as_millis() as u64, Ordering::SeqCst);

        for injection in AVAILABLE_INJECTIONS {
            send!(log, conn, drop_statement.as_str()).expect("don't let drop statement fail");
            send!(log, conn, create_statement.as_str()).expect("don't let create statement fail");
            info!(log, "{} ready to go!", injection);

            // NOTE: "1*" here, otherwise an index mutation is missing for each row insertion, thus cannot be detected.
            enable_failpoint(
                &log,
                client,
                config.status_addr.clone(),
                "github.com/pingcap/tidb/table/tables/corruptMutations",
                format!("1*return(\"{}\")", injection),
            )
            .await?;
            let row = table.new_row();
            let insertion = format!("INSERT INTO {} VALUES ({})", table.name, row.to_string());
            let update = format!(
                "UPDATE {} SET {} = {} WHERE {} = {}",
                table.name,
                table.cols[0].name,
                row.cols[0].next().to_string(),
                table.cols[1].name,
                row.cols[1].to_string()
            );

            let res = async {
                send!(log, conn, "begin optimistic")?;
                send!(log, conn, insertion.as_str())?;
                send!(log, conn, "commit")?;
                send!(log, conn, "begin optimistic")?;
                send!(log, conn, update.as_str())?;
                send!(log, conn, "commit")?;
                Ok(())
            }
            .await;

            if res.is_err() {
                send!(log, conn, "rollback")?;
            }
            info!(log, "workload finished"; "result" => ?res);

            collect_result(res, results, &table, injection, pool.clone()).await;
            disable_failpoint(
                &log,
                client,
                config.status_addr.clone(),
                "github.com/pingcap/tidb/table/tables/corruptMutations",
            )
            .await?;
        }

        Ok(())
    }
}

// similar to T2, but add a deletion after the update.
struct T3;
#[async_trait]
impl Workload for T3 {
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
        let duration = start.elapsed();
        CREATE_TABLE_DURAION_MS.fetch_add(duration.as_millis() as u64, Ordering::SeqCst);

        for injection in AVAILABLE_INJECTIONS {
            send!(log, conn, drop_statement.as_str()).expect("don't let drop statement fail");
            send!(log, conn, create_statement.as_str()).expect("don't let create statement fail");
            info!(log, "{} ready to go!", injection);

            // NOTE: "1*" here, otherwise an index mutation is missing for each row insertion, thus cannot be detected.
            enable_failpoint(
                &log,
                client,
                config.status_addr.clone(),
                "github.com/pingcap/tidb/table/tables/corruptMutations",
                format!("1*return(\"{}\")", injection),
            )
            .await?;
            send!(log, conn, "begin optimistic")?;
            let row = table.new_row();
            let insertion = format!("INSERT INTO {} VALUES ({})", table.name, row.to_string());
            let update = format!(
                "UPDATE {} SET {} = {} WHERE {} = {}",
                table.name,
                table.cols[0].name,
                row.cols[0].next().to_string(),
                table.cols[1].name,
                row.cols[1].to_string()
            );
            let deletion = format!(
                "DELETE FROM {} WHERE {} = {}",
                table.name,
                table.cols[0].name,
                row.cols[1].to_string()
            );

            let res = async {
                send!(log, conn, insertion.as_str())?;
                send!(log, conn, "commit")?;
                send!(log, conn, "begin optimistic")?;
                send!(log, conn, update.as_str())?;
                send!(log, conn, deletion.as_str())?;
                send!(log, conn, "commit")?;
                Ok(())
            }
            .await;

            if res.is_err() {
                send!(log, conn, "rollback")?;
            }
            info!(log, "workload finished"; "result" => ?res);

            collect_result(res, results, &table, injection, pool.clone()).await;
            disable_failpoint(
                &log,
                client,
                config.status_addr.clone(),
                "github.com/pingcap/tidb/table/tables/corruptMutations",
            )
            .await?;
        }

        Ok(())
    }
}

// similar to T2, but inject error in the update, instead of in the insertion.
struct T4;
#[async_trait]
impl Workload for T4 {
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
        let duration = start.elapsed();
        CREATE_TABLE_DURAION_MS.fetch_add(duration.as_millis() as u64, Ordering::SeqCst);

        for injection in AVAILABLE_INJECTIONS {
            send!(log, conn, drop_statement.as_str()).expect("don't let drop statement fail");
            send!(log, conn, create_statement.as_str()).expect("don't let create statement fail");
            info!(log, "{} ready to go!", injection);

            send!(log, conn, "begin optimistic")?;
            let row = table.new_row();
            let insertion = format!("INSERT INTO {} VALUES ({})", table.name, row.to_string());
            let update = format!(
                "UPDATE {} SET {} = {} WHERE {} = {}",
                table.name,
                table.cols[0].name,
                row.cols[0].next().to_string(),
                table.cols[1].name,
                row.cols[1].to_string()
            );

            let res = async {
                send!(log, conn, insertion.as_str())?;
                send!(log, conn, "commit")?;
                send!(log, conn, "begin optimistic")?;
                // NOTE: "1*" here, otherwise an index mutation is missing for each row insertion, thus cannot be detected.
                enable_failpoint(
                    &log,
                    client,
                    config.status_addr.clone(),
                    "github.com/pingcap/tidb/table/tables/corruptMutations",
                    format!("1*return(\"{}\")", injection),
                )
                .await
                .expect("failed to enable failpoint");
                send!(log, conn, update.as_str())?;
                send!(log, conn, "commit")?;
                Ok(())
            }
            .await;

            if res.is_err() {
                send!(log, conn, "rollback")?;
            }
            info!(log, "workload finished"; "result" => ?res);
            collect_result(res, results, &table, injection, pool.clone()).await;
            disable_failpoint(
                &log,
                client,
                config.status_addr.clone(),
                "github.com/pingcap/tidb/table/tables/corruptMutations",
            )
            .await?;
        }

        Ok(())
    }
}
