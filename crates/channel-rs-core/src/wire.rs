use std::fmt::Write;

use serde_json::Value;

use crate::ChannelError;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParsedMessage {
    pub id: Option<i64>,
    pub path: Option<String>,
    pub stream: Option<String>,
    pub sub: Option<String>,
    pub unsub: Option<String>,
    pub cancel: Option<i64>,
    pub topic: Option<String>,
    pub body_json: Option<String>,
    pub context_json: Option<String>,
    pub error_text: Option<String>,
    pub done: Option<bool>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct OutgoingMessage<'a> {
    pub id: Option<i64>,
    pub path: Option<&'a str>,
    pub stream: Option<&'a str>,
    pub sub: Option<&'a str>,
    pub unsub: Option<&'a str>,
    pub cancel: Option<i64>,
    pub topic: Option<&'a str>,
    pub body_json: Option<&'a str>,
    pub context_json: Option<&'a str>,
    pub error_text: Option<&'a str>,
    pub done: Option<bool>,
}

pub fn parse_messages(bytes: &[u8]) -> Result<Vec<ParsedMessage>, ChannelError> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }

    let first = bytes
        .iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace())
        .ok_or_else(|| ChannelError::new("empty payload"))?;

    match first {
        b'{' => Ok(vec![parse_one(&serde_json::from_slice::<Value>(bytes)?)?]),
        b'[' => {
            let value = serde_json::from_slice::<Value>(bytes)?;
            let Value::Array(items) = value else {
                return Err(ChannelError::new("invalid json message"));
            };
            items.iter().map(parse_one).collect()
        }
        _ => Err(ChannelError::new("expected json object or array")),
    }
}

fn parse_one(value: &Value) -> Result<ParsedMessage, ChannelError> {
    let Value::Object(object) = value else {
        return Err(ChannelError::new("invalid json message"));
    };

    let mut out = ParsedMessage::default();
    if let Some(field) = object.get("id") {
        out.id = Some(field.as_i64().ok_or_else(|| ChannelError::new("invalid id"))?);
    }
    if let Some(field) = object.get("path") {
        out.path = Some(
            field
                .as_str()
                .ok_or_else(|| ChannelError::new("invalid path"))?
                .to_string(),
        );
    }
    if let Some(field) = object.get("stream") {
        out.stream = Some(
            field
                .as_str()
                .ok_or_else(|| ChannelError::new("invalid stream"))?
                .to_string(),
        );
    }
    if let Some(field) = object.get("sub") {
        out.sub = Some(
            field
                .as_str()
                .ok_or_else(|| ChannelError::new("invalid sub"))?
                .to_string(),
        );
    }
    if let Some(field) = object.get("unsub") {
        out.unsub = Some(
            field
                .as_str()
                .ok_or_else(|| ChannelError::new("invalid unsub"))?
                .to_string(),
        );
    }
    if let Some(field) = object.get("cancel") {
        out.cancel = Some(field.as_i64().ok_or_else(|| ChannelError::new("invalid cancel"))?);
    }
    if let Some(field) = object.get("topic") {
        out.topic = Some(
            field
                .as_str()
                .ok_or_else(|| ChannelError::new("invalid topic"))?
                .to_string(),
        );
    }
    if let Some(field) = object.get("body") {
        out.body_json = Some(serde_json::to_string(field)?);
    }
    if let Some(field) = object.get("context") {
        out.context_json = Some(serde_json::to_string(field)?);
    }
    if let Some(field) = object.get("error") {
        out.error_text = Some(
            field
                .as_str()
                .ok_or_else(|| ChannelError::new("invalid error"))?
                .to_string(),
        );
    }
    if let Some(field) = object.get("done") {
        out.done = Some(field.as_bool().ok_or_else(|| ChannelError::new("invalid done"))?);
    }
    Ok(out)
}

pub fn encode_message(message: OutgoingMessage<'_>) -> Result<Vec<u8>, ChannelError> {
    encode_messages(&[message])
}

pub fn encode_messages(messages: &[OutgoingMessage<'_>]) -> Result<Vec<u8>, ChannelError> {
    let mut out = String::new();
    if messages.len() == 1 {
        write_object(&mut out, &messages[0])?;
    } else {
        out.push('[');
        for (index, message) in messages.iter().enumerate() {
            if index != 0 {
                out.push(',');
            }
            write_object(&mut out, message)?;
        }
        out.push(']');
    }
    Ok(out.into_bytes())
}

fn write_object(out: &mut String, message: &OutgoingMessage<'_>) -> Result<(), ChannelError> {
    out.push('{');
    let mut count = 0_usize;
    write_int_field(out, &mut count, "id", message.id);
    write_str_field(out, &mut count, "path", message.path)?;
    write_str_field(out, &mut count, "stream", message.stream)?;
    write_str_field(out, &mut count, "sub", message.sub)?;
    write_str_field(out, &mut count, "unsub", message.unsub)?;
    write_int_field(out, &mut count, "cancel", message.cancel);
    write_str_field(out, &mut count, "topic", message.topic)?;
    write_raw_field(out, &mut count, "body", message.body_json);
    write_raw_field(out, &mut count, "context", message.context_json);
    write_str_field(out, &mut count, "error", message.error_text)?;
    write_bool_field(out, &mut count, "done", message.done);
    out.push('}');
    Ok(())
}

fn write_prefix(out: &mut String, count: &mut usize, name: &str) -> Result<(), ChannelError> {
    if *count != 0 {
        out.push(',');
    }
    *count += 1;
    write!(out, "{}:", serde_json::to_string(name)?).map_err(|error| ChannelError::new(error.to_string()))
}

fn write_str_field(
    out: &mut String,
    count: &mut usize,
    name: &str,
    value: Option<&str>,
) -> Result<(), ChannelError> {
    let Some(value) = value else {
        return Ok(());
    };
    write_prefix(out, count, name)?;
    out.push_str(&serde_json::to_string(value)?);
    Ok(())
}

fn write_int_field(out: &mut String, count: &mut usize, name: &str, value: Option<i64>) {
    let Some(value) = value else {
        return;
    };
    let _ = write_prefix(out, count, name);
    let _ = write!(out, "{value}");
}

fn write_bool_field(out: &mut String, count: &mut usize, name: &str, value: Option<bool>) {
    let Some(value) = value else {
        return;
    };
    let _ = write_prefix(out, count, name);
    out.push_str(if value { "true" } else { "false" });
}

fn write_raw_field(out: &mut String, count: &mut usize, name: &str, value: Option<&str>) {
    let Some(value) = value else {
        return;
    };
    let _ = write_prefix(out, count, name);
    out.push_str(value);
}
