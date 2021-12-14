pub const MYSQL_ADDRESS: &str = "127.0.0.1:4000";
pub const AVAILABLE_INJECTIONS: &[&str] = &[
    "extraIndex",
    "missingIndex",
    "corruptIndexKey",
    "corruptIndexValue",
];

pub mod error;
pub mod failpoint;
pub mod table;
pub mod workload;

pub enum Effectiveness {
    Inconsistent, // the error message contains "inconsist"-like words
    OtherError,   // other errors are reported
    NoError,      // failed to detect error
    Consistent,   // the injections don't affect - e.g. `admin check table` returns no error
}