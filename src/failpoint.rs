use crate::Result;
use crate::FAILPOINT_DURATION_MS;
use reqwest;
use slog::error;
use slog::Logger;
use std::sync::atomic::Ordering;
use tokio::time;

pub async fn enable_failpoint(
    log: &Logger,
    client: &reqwest::Client,
    status_addr: impl Into<String>,
    name: impl Into<String>,
    value: impl Into<String>,
) -> Result<()> {
    let start = time::Instant::now();
    let res = client
        .put(format!(
            "http://{}/fail/{}",
            status_addr.into(),
            name.into()
        ))
        .body(value.into())
        .send()
        .await?;
    let duration = start.elapsed();
    FAILPOINT_DURATION_MS.fetch_add(duration.as_millis() as u64, Ordering::SeqCst);
    if let Err(e) = res.error_for_status_ref() {
        let status = res.status().as_u16();
        let text = res.text().await?;
        error!(log, "failed to enable failpoint"; "status code" => status, "text" => &text);
        return Err(e.into());
    }
    Ok(())
}

pub async fn disable_failpoint(
    log: &Logger,
    client: &reqwest::Client,
    status_addr: impl Into<String>,
    name: impl Into<String>,
) -> Result<()> {
    let start = time::Instant::now();
    let res = client
        .delete(format!(
            "http://{}/fail/{}",
            status_addr.into(),
            name.into()
        ))
        .send()
        .await?;
    let duration = start.elapsed();
    FAILPOINT_DURATION_MS.fetch_add(duration.as_millis() as u64, Ordering::SeqCst);
    if let Err(e) = res.error_for_status_ref() {
        let status = res.status().as_u16();
        let text = res.text().await?;
        error!(log, "failed to disable failpoint"; "status code" => status, "text" => &text);
        return Err(e.into());
    }
    Ok(())
}
