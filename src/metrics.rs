use std::sync::atomic::AtomicU64;

pub static FAILPOINT_DURATION_MS: AtomicU64 = AtomicU64::new(0);
pub static CREATE_TABLE_DURAION_MS: AtomicU64 = AtomicU64::new(0);
