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
