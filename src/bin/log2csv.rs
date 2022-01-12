use clap::{App, Arg};
use corrupttest::Result;
use regex::Regex;
use std::{fs::File, io::Read, path::Path};

fn main() -> Result<()> {
    let matches = App::new("corrupttest")
        .arg(
            Arg::new("log_dir_path")
                .short('p')
                .long("log_dir_path")
                .takes_value(true)
                .default_value("./logs"),
        )
        .get_matches();
    let dir_path = matches.value_of("log_dir_path").unwrap();

    println!("workload, mutation checker, assertion, injection, success, other error, failure, consistent");
    let dir = std::fs::read_dir(dir_path)?;
    for entry in dir {
        let entry = entry?;
        if entry.file_type()?.is_file() && entry.file_name().to_str().unwrap().ends_with(".log") {
            let res = one_file(entry.path())?;
            println!("{}", res.join("\n"));
        }
    }

    Ok(())
}

fn one_file(filepath: impl AsRef<Path>) -> Result<Vec<String>> {
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
        res.push(
            [
                workload_name.clone(),
                mutation_checker.clone(),
                assertion.clone(),
                captures.get(1).unwrap().as_str().to_owned(),
                captures.get(2).unwrap().as_str().to_owned(),
                captures.get(3).unwrap().as_str().to_owned(),
                captures.get(4).unwrap().as_str().to_owned(),
                captures.get(5).unwrap().as_str().to_owned(),
            ]
            .join(", "),
        );
    }
    Ok(res)
}
