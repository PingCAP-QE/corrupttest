#![feature(backtrace)]

// assume that injections won't corrupt table_ids. Otherwise drop table may not be able to clear corrupted data,
// and will affect following tests.
pub const AVAILABLE_INJECTIONS: &[&str] = &[
    "extraIndex",
    "missingIndex",
    "corruptIndexKey",
    "corruptIndexValue",
];

pub mod config;
pub mod error;
pub mod failpoint;
pub mod metrics;
pub mod table;
pub mod workload;

pub use metrics::*;

pub enum Effectiveness {
    Success,    // the error message contains "inconsist"-like words
    OtherError, // other errors are reported
    Failure,    // failed to detect error
    Consistent, // the injections don't affect - e.g. `admin check table` returns no error
}

pub type Result<T> = std::result::Result<T, error::MyError>;
