use crate::error::StringError;
use reqwest;

const STATUS_ADDRESS: &str = "127.0.0.1:10080";

pub async fn enable_failpoint(
    client: &reqwest::Client,
    name: impl Into<String>,
    value: impl Into<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let res = client
        .put(format!("http://{}/fail/{}", STATUS_ADDRESS, name.into()))
        .body(value.into())
        .send()
        .await?
        .text()
        .await?;
    if res.contains("fail") {
        return Err(Box::new(StringError(res)));
    }
    Ok(())
}

pub async fn disable_failpoint(
    client: &reqwest::Client,
    name: impl Into<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let res = client
        .delete(format!("http://{}/fail/{}", STATUS_ADDRESS, name.into()))
        .send()
        .await?
        .text()
        .await?;
    if res.contains("fail") {
        return Err(Box::new(StringError(res)));
    }
    Ok(())
}
