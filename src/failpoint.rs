use crate::error::MyError;
use crate::Result;
use crate::FAILPOINT_DURATION_MS;
use reqwest;
use std::sync::atomic::Ordering;
use tokio::time;

const STATUS_ADDRESS: &str = "127.0.0.1:10080";

pub async fn enable_failpoint(
    client: &reqwest::Client,
    name: impl Into<String>,
    value: impl Into<String>,
) -> Result<()> {
    let start = time::Instant::now();
    let res = client
        .put(format!("http://{}/fail/{}", STATUS_ADDRESS, name.into()))
        .body(value.into())
        .send()
        .await?
        .text()
        .await?;
    let duration = start.elapsed();
    FAILPOINT_DURATION_MS.fetch_add(duration.as_millis() as u64, Ordering::SeqCst);
    if res.contains("fail") {
        return Err(MyError::StringError(res));
    }
    Ok(())
}

pub async fn disable_failpoint(client: &reqwest::Client, name: impl Into<String>) -> Result<()> {
    let start = time::Instant::now();
    let res = client
        .delete(format!("http://{}/fail/{}", STATUS_ADDRESS, name.into()))
        .send()
        .await?
        .text()
        .await?;
    let duration = start.elapsed();
    FAILPOINT_DURATION_MS.fetch_add(duration.as_millis() as u64, Ordering::SeqCst);
    if res.contains("fail") {
        return Err(MyError::StringError(res));
    }
    Ok(())
}
