use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::{ChannelError, ChannelMessage};

pub fn deserialize_messages(bytes: &[u8]) -> Result<Vec<ChannelMessage>, ChannelError> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }

    let first = bytes
        .iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace())
        .ok_or_else(|| ChannelError::new("empty payload"))?;

    match first {
        b'[' => Ok(serde_json::from_slice(bytes)?),
        b'{' => Ok(vec![serde_json::from_slice(bytes)?]),
        _ => Err(ChannelError::new("expected json object or array")),
    }
}

pub fn serialize_message(message: &ChannelMessage) -> Result<Vec<u8>, ChannelError> {
    Ok(serde_json::to_vec(message)?)
}

pub fn serialize_messages(messages: &[ChannelMessage]) -> Result<Vec<u8>, ChannelError> {
    Ok(serde_json::to_vec(messages)?)
}

pub fn deserialize_body<T: DeserializeOwned>(value: &Option<Value>) -> Result<Option<T>, ChannelError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(raw) => Ok(Some(serde_json::from_value(raw.clone())?)),
    }
}

pub fn to_json_value<T: Serialize>(value: &T) -> Result<Value, ChannelError> {
    Ok(serde_json::to_value(value)?)
}
