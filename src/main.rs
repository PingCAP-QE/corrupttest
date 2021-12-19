use corrupttest::{
    config::init_app, table::*, workload::find_workload, Effectiveness, AVAILABLE_INJECTIONS,
    MYSQL_ADDRESS,
};
use futures::{pin_mut, StreamExt};
use mysql::{prelude::*, Opts, Pool};
use std::{collections::HashMap, time};

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let config = init_app();
    let workload = find_workload(&config.workload_name);
    let client = reqwest::Client::new();
    let url = format!("mysql://root@{}/test", MYSQL_ADDRESS);
    println!("{}", &url);
    let pool = Pool::new(Opts::from_url(&url)?)?;
    let mut conn = pool.get_conn().unwrap();
    conn.query_drop("SET GLOBAL tidb_txn_mode = 'optimistic';")?;
    conn.query_drop("SET tidb_txn_mode = 'optimistic';")?;
    let tables = Table::stream();
    pin_mut!(tables);
    // {table} x {workload} x {injection} -> (success, other success, failure)
    let mut results = HashMap::<(Table, String, String), Effectiveness>::new();
    let mut cnt = 0;
    let start = time::Instant::now();
    while let Some(table) = tables.next().await {
        cnt += 1;
        workload
            .execute(&config, table, &client, &mut conn, &mut results)
            .await?;
        println!(
            "table per second: {}",
            cnt as f32 / start.elapsed().as_secs_f32()
        );
    }
    print_result(cnt, results);
    Ok(())
}

fn print_result(cnt: i32, results: HashMap<(Table, String, String), Effectiveness>) {
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
            "{}:\tsuccess:{}\tother success:{}\tfailure:{}\tconsistent:{}",
            injection, counts.0, counts.1, counts.2, counts.3
        );
    }
}
