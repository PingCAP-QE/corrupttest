use std::backtrace::Backtrace;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum MyError {
    #[error("{0}")]
    StringError(String),
    #[error("sqlx error: {sqlx:?}")]
    SqlxError {
        #[from]
        sqlx: sqlx::Error,
        backtrace: Backtrace,
    },
    #[error("reqwest error: {reqwest:?}")]
    ReqwestError {
        #[from]
        reqwest: reqwest::Error,
        backtrace: Backtrace,
    },
    #[error("{0}")]
    Io(#[from] std::io::Error),
}
