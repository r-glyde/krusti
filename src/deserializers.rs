use std::str;

use serde_json::Value as JsonValue;

use crate::schema_registry;
use crate::utils;

// TODO: better error handling for these
pub fn avro_deserializer(
    bytes: Option<Vec<u8>>,
    registry_decoder: &mut schema_registry::Decoder,
) -> JsonValue {
    utils::json_from_avro(
        registry_decoder
            .decode(bytes.as_ref().map(|bs| bs.as_slice()))
            .unwrap()
    )
    .unwrap()
}

pub fn string_deserializer(bytes: Option<Vec<u8>>) -> JsonValue {
    bytes.map_or_else(
        || JsonValue::Null,
        |bs| match serde_json::from_slice(bs.as_slice()) {
            Ok(json) => json,
            Err(_) => JsonValue::String(str::from_utf8(bs.as_slice()).unwrap().to_owned()),
        },
    )
}
