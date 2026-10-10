use std::any::{Any, TypeId};

use serde::{Serialize, de::DeserializeOwned};

use crate::ChannelError;

pub fn is_unit<T: 'static>() -> bool {
    TypeId::of::<T>() == TypeId::of::<()>()
}

pub fn encode_optional<T>(value: &T) -> Result<Option<String>, ChannelError>
where
    T: Serialize + 'static,
{
    if is_unit::<T>() {
        return Ok(None);
    }
    Ok(Some(serde_json::to_string(value)?))
}

pub fn encode_response<T>(value: Option<&T>) -> Result<Option<String>, ChannelError>
where
    T: Serialize + 'static,
{
    match value {
        None => Ok(None),
        Some(value) => encode_optional(value),
    }
}

/// Decodes a JSON field. A missing field decodes as `null` (so `Option<T>`,
/// `serde_json::Value` and `()` accept it), matching TS where an absent body
/// is just `undefined`. Errors name the field (`what`) instead of exposing
/// serde's internal message to the peer.
pub fn decode_field<T>(json: Option<&str>, what: &str) -> Result<T, ChannelError>
where
    T: DeserializeOwned + 'static,
{
    if is_unit::<T>() {
        return Ok(serde_json::from_str("null")?);
    }
    serde_json::from_str(json.unwrap_or("null")).map_err(|_| ChannelError::new(format!("invalid {what}")))
}

/// Decodes a request context. TS hands handlers `context ?? {}`, so a
/// missing context is tried as `{}` first and then as `null`.
pub fn decode_context<T>(json: Option<&str>) -> Result<T, ChannelError>
where
    T: DeserializeOwned + 'static,
{
    match json {
        Some(raw) => decode_field(Some(raw), "context"),
        None => decode_field(Some("{}"), "context").or_else(|_| decode_field(None, "context")),
    }
}

pub fn topic_from_value<T>(value: &T) -> Result<String, ChannelError>
where
    T: Serialize + 'static,
{
    let any = value as &dyn Any;
    if let Some(text) = any.downcast_ref::<String>() {
        return Ok(text.clone());
    }
    if let Some(text) = any.downcast_ref::<&'static str>() {
        return Ok((*text).to_string());
    }
    Ok(serde_json::to_string(value)?)
}
