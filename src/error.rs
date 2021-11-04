use std::fmt::Display;

#[derive(Debug, Clone)]
pub struct StringError(pub String);

impl Display for StringError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for StringError {}
