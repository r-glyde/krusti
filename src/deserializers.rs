use std::str;

use serde_json::Value as JsonValue;

use crate::utils;

// TODO: better error handling for these
pub fn avro_deserializer(
    bytes: Option<Vec<u8>>,
    registry_decoder: &mut schema_registry_converter::Decoder,
) -> JsonValue {
    utils::json_from_avro(
        registry_decoder
            .decode(bytes.as_ref().map(|bs| bs.as_slice()))
            .unwrap(),
    )
    .unwrap()
}

pub fn string_deserializer(bytes: Option<Vec<u8>>) -> JsonValue {
    bytes.map_or_else(
        || JsonValue::Null,
        |bs| match str::from_utf8(bs.as_slice()) {
            Ok(v) => serde_json::from_str(&v).unwrap(),
            Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
        },
    )
}
