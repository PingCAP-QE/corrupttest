use crate::error::StringError;
use reqwest;

const STATUS_ADDRESS: &'static str = "127.0.0.1:10080";

pub fn enable_failpoint(
    name: impl Into<String>,
    value: impl Into<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let res = reqwest::blocking::Client::new()
        .put(format!("http://{}/fail/{}", STATUS_ADDRESS, name.into()))
        .body(value.into())
        .send()?
        .text()?;
    if res.contains("fail") {
        return Err(Box::new(StringError(res)));
    }
    Ok(())
}

pub fn disable_failpoint(name: impl Into<String>) -> Result<(), Box<dyn std::error::Error>> {
    let _ = reqwest::blocking::Client::new()
        .delete(format!("http://{}/fail/{}", STATUS_ADDRESS, name.into()))
        .send()?
        .text()?;
    Ok(())
}
