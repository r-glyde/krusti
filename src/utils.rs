use std::iter::FromIterator;

use avro_rs::types::{ToAvro, Value as AvroValue};
use serde_json::json;
use serde_json::Value as JsonValue;

pub fn json_from_avro(value: AvroValue) -> Result<JsonValue, String> {
    match value {
        AvroValue::Null => Ok(JsonValue::Null),
        AvroValue::Boolean(b) => Ok(JsonValue::Bool(b)),
        AvroValue::String(s) => Ok(JsonValue::String(s)),
        AvroValue::Int(n) => Ok(json!(n)),
        AvroValue::Long(n) => Ok(json!(n)),
        AvroValue::Float(n) => Ok(json!(n)),
        AvroValue::Double(n) => Ok(json!(n)),
        AvroValue::Array(vs) => Ok(JsonValue::Array(
            vs.into_iter().map(|v| json_from_avro(v).unwrap()).collect(),
        )),
        // TODO: this and/or record is probably wrong
        AvroValue::Map(kvs) => Ok(JsonValue::Object(serde_json::map::Map::from_iter(
            kvs.into_iter()
                .map(|(k, v)| (k, json_from_avro(v).unwrap())),
        ))),
        AvroValue::Enum(_, s) => Ok(json!(s)),
        AvroValue::Union(v) => json_from_avro(v.avro()),
        AvroValue::Record(fields) => Ok(JsonValue::Object(serde_json::map::Map::from_iter(
            fields
                .into_iter()
                .map(|(k, v)| (k, json_from_avro(v).unwrap())),
        ))),
        _ => Err("Don't know what to do with: value".to_string()),
    }
}

// TODO: complete below
// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn converts_string() {
//         let data = AvroValue::String("hello".to_string());
//         assert_eq!(JsonValue::String("hello".to_string()), json_from_avro(data))
//     }
// }
