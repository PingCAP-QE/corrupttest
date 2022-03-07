#[macro_use]
extern crate prettytable;
use corrupttest::{
    config::{init_app, Config},
    table::*,
    workload::find_workload,
    Effectiveness, Result, AVAILABLE_INJECTIONS, CREATE_TABLE_DURAION_MS, FAILPOINT_DURATION_MS,
};
use futures::{pin_mut, StreamExt};
use slog::{info, o, Drain, Logger};
use sqlx::mysql::MySqlPoolOptions;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time,
};

static EXIT: AtomicBool = AtomicBool::new(false);

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let config = init_app();
    let log = init_logger(&config);
    let workload = find_workload(&config.workload_name);
    let (client, pool) = init_pool(&log, &config).await?;
    ctrlc::set_handler(move || {
        EXIT.store(true, Ordering::SeqCst);
    })?;
    info!(log, "initialized"; "config" => ?config);

    let tables = Table::stream();
    pin_mut!(tables);
    // {table} x {workload} x {injection} -> (success, other success, failure)
    let mut results = HashMap::<(Table, String, String), Effectiveness>::new();
    let mut cnt = 0;
    let start = time::Instant::now();
    while let Some(table) = tables.next().await {
        if EXIT.load(Ordering::SeqCst) {
            break;
        }
        if config.limit > 0 && cnt >= config.limit {
            break;
        }
        cnt += 1;
        workload
            .execute(
                log.clone(),
                &config,
                table,
                &client,
                pool.clone(),
                &mut results,
            )
            .await?;
        info!(
            log,
            "stats";
            "current" => cnt,
            "table per second" => cnt as f32 / start.elapsed().as_secs_f32()
        );
    }
    print_result(log, &config, cnt, results);
    Ok(())
}

async fn init_pool(
    log: &Logger,
    config: &Config,
) -> Result<(reqwest::Client, Arc<sqlx::Pool<sqlx::MySql>>)> {
    let client = reqwest::Client::new();
    info!(log, "using tidb {}", config.uri);
    let pool = MySqlPoolOptions::new()
        .max_connections(32)
        .connect(&config.uri)
        .await?;
    let pool = Arc::new(pool);
    Ok((client, pool))
}

fn init_logger(config: &Config) -> Logger {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&config.log_path)
        .unwrap();
    let file_decorator = slog_term::PlainSyncDecorator::new(file);
    // also print error logs to stderr
    let stderr_decorator = slog_term::TermDecorator::new().build();
    let file_drain = slog_term::FullFormat::new(file_decorator)
        .use_file_location()
        .build();
    let stderr_drain = slog_async::Async::new(
        slog::LevelFilter::new(
            slog_term::FullFormat::new(stderr_decorator)
                .use_file_location()
                .build(),
            slog::Level::Warning,
        )
        .fuse(),
    )
    .build();
    let drain = slog::Duplicate::new(file_drain, stderr_drain).fuse();
    slog::Logger::root(drain, o!())
}

fn print_result(
    log: Logger,
    config: &Config,
    cnt: u32,
    results: HashMap<(Table, String, String), Effectiveness>,
) {
    info!(log, "printing result"; 
        "workload" => &config.workload_name, 
        "total tables" => cnt, 
        "DDL duration" => CREATE_TABLE_DURAION_MS.load(Ordering::SeqCst), 
        "failpoint duration" => FAILPOINT_DURATION_MS.load(Ordering::SeqCst));
    let mut table = prettytable::Table::new();
    table.add_row(row![
        "injection",
        "success",
        "other error",
        "failure",
        "consistent",
    ]);
    for &injection in AVAILABLE_INJECTIONS {
        let counts = results
            .iter()
            .filter(|(key, _)| key.2.as_str() == injection)
            .fold((0, 0, 0, 0), |acc, (_, value)| match value {
                Effectiveness::Success => (acc.0 + 1, acc.1, acc.2, acc.3),
                Effectiveness::OtherError => (acc.0, acc.1 + 1, acc.2, acc.3),
                Effectiveness::Failure => (acc.0, acc.1, acc.2 + 1, acc.3),
                Effectiveness::Consistent => (acc.0, acc.1, acc.2, acc.3 + 1),
            });
        info!(
            log,
            "{}:\tsuccess:{}\tother success:{}\tfailure:{}\tconsistent:{}",
            injection,
            counts.0,
            counts.1,
            counts.2,
            counts.3
        );
        table.add_row(row![injection, counts.0, counts.1, counts.2, counts.3,]);
    }
    table.printstd();
}
