use core::fmt::Debug;

use rdkafka::message::{BorrowedMessage, Message};
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

impl Record {
    pub fn new(msg: BorrowedMessage, key: JsonValue, value: JsonValue) -> Record {
        Record {
            key,
            value,
            topic: msg.topic().to_owned(),
            partition: msg.partition(),
            offset: msg.offset(),
            timestamp: msg.timestamp().to_millis().unwrap(),
        }
    }
}
