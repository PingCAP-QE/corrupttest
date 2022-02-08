use clap::{App, Arg};
use corrupttest::{error::MyError, Result};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::{fs::File, io::Read, path::Path};

const RESULT_URL: &str = "/pingcap/qa/tests/corrupttest/res.csv";
const RESULT_FILENAME: &str = "res.csv";

// sort by mutation checker, assertion, injection, workload
fn main() -> Result<()> {
    let matches = App::new("corrupttest")
        .arg(
            Arg::new("log_dir_path")
                .short('p')
                .long("log_dir_path")
                .takes_value(true)
                .default_value("./logs"),
        )
        .arg(
            Arg::new("local")
                .short('l')
                .long("local")
                .takes_value(false)
                .help("run locally, don't compare with or update remote results"),
        )
        .get_matches();
    let dir_path = matches.value_of("log_dir_path").unwrap();
    let local = matches.is_present("local");

    // collect results from logs
    let dir = std::fs::read_dir(dir_path)?;
    let mut records = Vec::new();
    for entry in dir {
        let entry = entry?;
        if entry.file_type()?.is_file() && entry.file_name().to_str().unwrap().ends_with(".log") {
            records.append(&mut one_file(entry.path())?);
        }
    }
    process_results(records, local)?;
    Ok(())
}

fn process_results(mut records: Vec<Record>, local: bool) -> Result<()> {
    let record_sort_key = |r: &Record| {
        (
            r.mutation_checker.clone(),
            r.assertion.clone(),
            r.injection.clone(),
            r.workload.clone(),
        )
    };
    records.sort_by_cached_key(record_sort_key);

    if local {
        write_result(&records)?;
    } else {
        // get previous results from remote
        let output = std::process::Command::new("dodo")
            .env_remove("all_proxy")
            .env_remove("http_proxy")
            .env_remove("https_proxy")
            .arg("get")
            .arg(RESULT_URL)
            .output()
            .expect("failed to get previous result");
        if !output.status.success() {
            println!("{:?}", output);
            return Err(MyError::StringError(
                "failed to get previous result".to_string(),
            ));
        }
        let mut old_record = {
            let file = std::fs::File::open(RESULT_FILENAME)?;
            let mut rdr = csv::Reader::from_reader(file);
            rdr.deserialize()
                .map(|result: std::result::Result<Record, _>| result.unwrap())
                .collect::<Vec<_>>()
        };

        // Compare it with current one
        if old_record.len() != records.len() {
            println!("{:?}", old_record);
            println!("{:?}", records);
            return Err(MyError::StringError(
                "results have different lengths".to_string(),
            ));
        }
        old_record.sort_by_cached_key(record_sort_key);
        let diffs: Vec<_> = old_record
            .iter()
            .zip(records.iter())
            .filter(|(old, new)| {
                old.effective_rate != new.effective_rate
                    && !(old.effective_rate.is_nan() && new.effective_rate.is_nan())
            })
            .collect();
        if !diffs.is_empty() {
            for (old, new) in &diffs {
                println!(
                    "mutation_checker:{}, assertion:{}, injection:{}, workload:{}, {} -> {}",
                    old.mutation_checker,
                    old.assertion,
                    old.injection,
                    old.workload,
                    old.effective_rate,
                    new.effective_rate
                );
            }
        }

        write_result(&records)?;

        // upload to remote for future comparison
        let output = std::process::Command::new("dodo")
            .env_remove("all_proxy")
            .env_remove("http_proxy")
            .env_remove("https_proxy")
            .arg("put")
            .arg(RESULT_URL)
            .arg(RESULT_FILENAME)
            .output()
            .expect("failed to put new result");
        if !output.status.success() {
            println!("{:?}", output);
            return Err(MyError::StringError("failed to put new result".to_string()));
        }
        if !diffs.is_empty() {
            return Err(MyError::StringError(
                "effective rates have changed".to_string(),
            ));
        }
    }

    Ok(())
}

fn write_result(records: &Vec<Record>) -> Result<()> {
    let file = std::fs::File::create(RESULT_FILENAME)?;
    let mut wtr = csv::Writer::from_writer(file);
    for record in records {
        wtr.serialize(record).unwrap();
    }
    Ok(())
}

#[derive(Serialize, Deserialize, Debug)]
struct Record {
    mutation_checker: String,
    assertion: String,
    injection: String,
    workload: String,
    success: u32,
    other_error: u32,
    failure: u32,
    consistent: u32,
    effective_rate: f32,
}

fn one_file(filepath: impl AsRef<Path>) -> Result<Vec<Record>> {
    let mut file = File::open(filepath)?;
    let mut content = String::new();
    file.read_to_string(&mut content)?;

    let mut res = vec![];
    let mut lines = content.lines();
    // 2nd line, the config
    lines.next().unwrap();
    let line = lines.next().unwrap();
    let re = Regex::new(r#"workload_name: "(.*?)", mutation_checker: "(.*?)", assertion: "(.*?)""#)
        .unwrap();
    let captures = re.captures(line).unwrap();
    let workload_name = captures.get(1).unwrap().as_str().to_owned();
    let mutation_checker = captures.get(2).unwrap().as_str().to_owned();
    let assertion = captures.get(3).unwrap().as_str().to_owned();

    // the last 4 lines
    let lines = lines.rev().take(4);
    for line in lines {
        let re = Regex::new(
            r#"\]\s+(.*?):\s+success:(\d+)\s+other success:(\d+)\s+failure:(\d+)\s+consistent:(\d+).*"#,
        )
        .unwrap();
        let captures = re.captures(line).unwrap();
        let injection = captures.get(1).unwrap().as_str().to_owned();
        let success = captures.get(2).unwrap().as_str().parse::<u32>().unwrap();
        let other_error = captures.get(3).unwrap().as_str().parse::<u32>().unwrap();
        let failure = captures.get(4).unwrap().as_str().parse::<u32>().unwrap();
        let consistent = captures.get(5).unwrap().as_str().parse::<u32>().unwrap();
        let effective_rate = success as f32 / (success + failure) as f32;
        let record = Record {
            mutation_checker: mutation_checker.clone(),
            assertion: assertion.clone(),
            injection,
            workload: workload_name.clone(),
            success,
            other_error,
            failure,
            consistent,
            effective_rate,
        };
        res.push(record);
    }
    Ok(res)
}
