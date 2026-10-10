use std::fmt::Write;

use serde_json::Value;

use crate::ChannelError;

/// A request id as it appeared on the wire. TS accepts any JSON value as an
/// id and echoes it back unchanged, so non-integer ids are kept as raw JSON.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WireId {
    Int(i64),
    Raw(String),
}

impl WireId {
    pub fn as_int(&self) -> Option<i64> {
        match self {
            WireId::Int(value) => Some(*value),
            WireId::Raw(_) => None,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParsedMessage {
    pub id: Option<WireId>,
    pub path: Option<String>,
    pub stream: Option<String>,
    pub sub: Option<String>,
    pub unsub: Option<String>,
    pub cancel: Option<WireId>,
    pub topic: Option<String>,
    pub body_json: Option<String>,
    pub context_json: Option<String>,
    pub error_text: Option<String>,
    pub done: Option<bool>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct OutgoingMessage<'a> {
    pub id: Option<i64>,
    /// Raw JSON id echoed back for requests whose id was not an integer.
    pub id_raw: Option<&'a str>,
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

impl<'a> OutgoingMessage<'a> {
    /// A response addressed to `id`, whatever its JSON type was.
    pub fn reply(id: &'a WireId) -> Self {
        match id {
            WireId::Int(value) => Self {
                id: Some(*value),
                ..Self::default()
            },
            WireId::Raw(raw) => Self {
                id_raw: Some(raw),
                ..Self::default()
            },
        }
    }
}

/// Parses one WebSocket text payload. Only invalid JSON is an error (TS
/// closes the connection for that too); wrong-typed fields are handled the
/// way the TS reference's truthiness checks handle them.
pub fn parse_messages(bytes: &[u8]) -> Result<Vec<ParsedMessage>, ChannelError> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }

    let value = serde_json::from_slice::<Value>(bytes)
        .map_err(|_| ChannelError::new("invalid json message"))?;
    match value {
        Value::Array(items) => Ok(items.iter().filter_map(parse_one).collect()),
        value => Ok(parse_one(&value).into_iter().collect()),
    }
}

fn is_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(number) => number.as_f64().is_some_and(|value| value != 0.0),
        Value::String(text) => !text.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

/// A string-valued routing field (`path`, `stream`, `sub`, ...). TS checks
/// these for truthiness and then uses them as map keys, so a falsy value is
/// absent and any other non-string value becomes its JSON text (which then
/// fails the lookup with "api not found" instead of killing the connection).
fn string_field(object: &serde_json::Map<String, Value>, name: &str) -> Option<String> {
    let field = object.get(name)?;
    if !is_truthy(field) {
        return None;
    }
    match field {
        Value::String(text) => Some(text.clone()),
        other => Some(other.to_string()),
    }
}

fn id_field(object: &serde_json::Map<String, Value>, name: &str) -> Option<WireId> {
    let field = object.get(name)?;
    Some(match field.as_i64() {
        Some(value) => WireId::Int(value),
        None => WireId::Raw(field.to_string()),
    })
}

fn parse_one(value: &Value) -> Option<ParsedMessage> {
    let Value::Object(object) = value else {
        return None;
    };

    let error_text = object.get("error").filter(|field| is_truthy(field)).map(|field| match field {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    });
    Some(ParsedMessage {
        id: id_field(object, "id"),
        path: string_field(object, "path"),
        stream: string_field(object, "stream"),
        sub: string_field(object, "sub"),
        unsub: string_field(object, "unsub"),
        cancel: id_field(object, "cancel"),
        topic: string_field(object, "topic"),
        body_json: object.get("body").map(Value::to_string),
        context_json: object.get("context").map(Value::to_string),
        error_text,
        done: object.get("done").map(is_truthy),
    })
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
    if message.id.is_none() {
        write_raw_field(out, &mut count, "id", message.id_raw);
    }
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
