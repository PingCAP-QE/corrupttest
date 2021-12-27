# corrupttest

A tool to test the ft-data-inconsistency feature. It enumerates combinations of table structures and run custom workloads on them. Certain types of errors are injected in the workload, and we check if the feature detects them.

## Requirements

nightly rust

## Usage

Detailed usage:
```
❯ cargo run -- -h

corrupttest 

USAGE:
    corrupttest [OPTIONS] --assertion <assertion> --mutation_checker <mutation_checker> --workload <workload>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -a, --assertion <assertion>                   [possible values: off, fast, strict]
    -l, --limit <limit>                           [default: 0]
    -o, --log_path <log_path>                     [default: corrupttest.log]
    -m, --mutation_checker <mutation_checker>     [possible values: 0, 1, true, false, on, off]
    -u, --uri <uri>                               [default: mysql://root@127.0.0.1:4000/test]
    -w, --workload <workload>                     [possible values: single, t2, t4, double, t3]
```
The feature flags and a workload are required. 

Examples: 

`cargo +nightly run -- -a strict -m 1 -w t2`

`cargo +nightly run -- -a fast -m 1 -w single -o logs/single.log -l 100` only runs the first 100 tables.

There is a [script](./enumerate.sh) to run all combinations of the flags, modify them when needed.

## For developers

People who want to add/maintain the tests most likely need to modify the following files:

`table.rs`: it defines components of a table and their generators. It utilizes the `async-stream` crate to write coroutine-like generators.

`workload.rs`: it defines all available workloads. When adding a new one, remember to also add it to the `WORKLOADS` static map.

`main.rs`: main testing logic.

### test design

The tests run in serial since we don't want to mess up with failpoints. A test on a simple workload may look like this.

```
                              ┌──genearte new table◄────┐
      ┌───────────────────────┤                         ├───────────────────┐
      │                       └──choose an injection◄───┘                   │
      │                                                                     │
      │                                                                     │
      │                                                                     │
      │                                                                     │
      ▼                                                                     │
drop table────►create table──────►run SQLs──────►disable failpoint─────►collect result
                                   │   ▲
                                   │   │
                                   │   │
                                   ▼   │
                             enable some failpoint
```

Notes on performance:

Creating and dropping tables are slow so the tests can take a long time. An empirical number is 3-5 loops per second. There are over 3000 tables in the first version.