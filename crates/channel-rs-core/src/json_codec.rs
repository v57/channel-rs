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

pub fn decode_required<T>(json: Option<&str>) -> Result<T, ChannelError>
where
    T: DeserializeOwned + 'static,
{
    if is_unit::<T>() {
        return Ok(serde_json::from_str("null")?);
    }

    let raw = json.ok_or_else(|| ChannelError::new("missing json field"))?;
    Ok(serde_json::from_str(raw)?)
}

pub fn decode_optional<T>(json: Option<&str>) -> Result<Option<T>, ChannelError>
where
    T: DeserializeOwned + 'static,
{
    match json {
        None => Ok(None),
        Some(raw) => Ok(Some(serde_json::from_str(raw)?)),
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
