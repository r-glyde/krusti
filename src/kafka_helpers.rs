use core::fmt::Debug;

use serde::Serialize;
use serde_json::Value as JsonValue;

#[derive(Debug)]
pub struct Offsets {
    pub low: i64,
    pub high: i64,
}

#[derive(Serialize)]
pub struct Record {
    pub key: JsonValue,
    pub value: JsonValue,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
}
