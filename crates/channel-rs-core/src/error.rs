use thiserror::Error;

#[derive(Debug, Clone, Error, PartialEq, Eq)]
#[error("{message}")]
pub struct ChannelError {
    message: String,
}

impl ChannelError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl From<std::io::Error> for ChannelError {
    fn from(value: std::io::Error) -> Self {
        Self::new(value.to_string())
    }
}

impl From<serde_json::Error> for ChannelError {
    fn from(value: serde_json::Error) -> Self {
        Self::new(value.to_string())
    }
}
