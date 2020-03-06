use avro_rs::types::Value;
use avro_rs::{from_avro_datum, Schema};
use byteorder::{BigEndian, ReadBytesExt};
use curl::easy::{Easy2, Handler, WriteError};
use serde_json::{Value as JsonValue};
use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::error::Error;
use std::io::Cursor;
use std::ops::Deref;
use std::str;
use url::Url;

// Largely taken from: https://github.com/gklijs/schema_registry_converter/tree/master/src

#[derive(Debug)]
pub struct Decoder {
    schema_registry_url: String,
    cache: &'static mut HashMap<u32, Result<Schema, SRCError>, RandomState>,
}

impl Decoder {

    pub fn new(schema_registry_url: String) -> Decoder {
        let new_cache = Box::new(HashMap::new());
        Decoder {
            schema_registry_url,
            cache: Box::leak(new_cache),
        }
    }

    pub fn remove_errors_from_cache(&mut self) {
        self.cache.retain(|_, v| v.is_ok());
    }

    pub fn decode(&mut self, bytes: Option<&[u8]>) -> Result<Value, SRCError> {
        match bytes {
            None => Ok(Value::Null),
            Some(p) if p.len() > 4 && p[0] == 0 => self.deserialize(p),
            Some(p) => Ok(Value::Bytes(p.to_vec())),
        }
    }

    fn deserialize<'a>(&'a mut self, bytes: &'a [u8]) -> Result<Value, SRCError> {
        let schema = self.get_schema(bytes);
        let mut reader = Cursor::new(&bytes[5..]);
        match schema {
            Ok(v) => match from_avro_datum(&v, &mut reader, None) {
                Ok(v) => Ok(v),
                Err(e) => Err(SRCError::new(
                    "Could not transform bytes using schema",
                    Some(&e.to_string()),
                    false,
                )),
            },
            Err(e) => Err(e.clone()),
        }
    }

    fn get_schema(&mut self, bytes: &[u8]) -> &mut Result<Schema, SRCError> {
        let mut buf = &bytes[1..5];
        let id = buf.read_u32::<BigEndian>().unwrap();
        let sr = &self.schema_registry_url;
        self.cache
            .entry(id)
            .or_insert_with(|| match get_schema_by_id(id, sr) {
                Ok(v) => Ok(v),
                Err(e) => Err(e.into_cache()),
            })
    }
}

pub fn get_schema_by_id(id: u32, schema_registry_url: &str) -> Result<Schema, SRCError> {
  let url = Url::parse(schema_registry_url)
      .map_err(|e| SRCError {
          error: "Error parsing schema registry url".into(),
          side: Some(format!("{}", e)),
          retriable: false,
          cached: false,
      })?
      .join("/schemas/ids/")
      .map_err(|e| SRCError {
          error: "Error constructing schema registry url".into(),
          side: Some(format!("{}", e)),
          retriable: false,
          cached: false,
      })?
      .join(&id.to_string())
      .map_err(|e| SRCError {
          error: "Error constructing schema registry url".into(),
          side: Some(format!("{}", e)),
          retriable: false,
          cached: false,
      })?
      .into_string();
  schema_from_url(&url, Option::from(id)).and_then(|t| Ok(t.0))
}

fn schema_from_url(url: &str, id: Option<u32>) -> Result<(Schema, u32), SRCError> {
  let easy = match perform_get(url) {
      Ok(v) => v,
      Err(e) => {
          return Err(SRCError::new(
              "error performing get to schema registry",
              Some(e.description()),
              true,
          ))
      }
  };
  let json: JsonValue = match to_json(easy) {
      Ok(v) => v,
      Err(e) => return Err(e),
  };
  let raw_schema = match json["schema"].as_str() {
      Some(v) => v,
      None => {
          return Err(SRCError::new(
              "Could not get raw schema from response",
              None,
              false,
          ))
      }
  };
  let schema = match Schema::parse_str(raw_schema) {
      Ok(v) => v,
      Err(e) => {
          return Err(SRCError::new(
              "Could not parse schema",
              Some(&e.to_string()),
              false,
          ))
      }
  };
  let id = match id {
      Some(v) => v,
      None => {
          let id_from_response = match json["id"].as_u64() {
              Some(v) => v,
              None => return Err(SRCError::new("Could not get id from response", None, false)),
          };
          id_from_response as u32
      }
  };
  Ok((schema, id))
}

fn perform_get(url: &str) -> Result<Easy2<Collector>, curl::Error> {
  let mut easy = Easy2::new(Collector(Vec::new()));
  easy.get(true)?;
  easy.url(url)?;
  easy.perform()?;
  Ok(easy)
}

fn to_json(mut easy: Easy2<Collector>) -> Result<JsonValue, SRCError> {
  match easy.response_code() {
      Ok(200) => (),
      Ok(v) => {
          return Err(SRCError::new(
              format!("Did not get a 200 response code but {} instead", v).as_str(),
              None,
              false,
          ))
      }
      Err(e) => {
          return Err(SRCError::new(
              format!("Encountered error getting http response: {}", e).as_str(),
              Some(e.description()),
              true,
          ))
      }
  }
  let mut data = Vec::new();
  match easy.get_ref() {
      Collector(b) => data.extend_from_slice(b),
  }
  let body = match str::from_utf8(data.as_ref()) {
      Ok(v) => v,
      Err(e) => {
          return Err(SRCError::new(
              "Invalid UTF-8 sequence",
              Some(e.description()),
              false,
          ))
      }
  };
  match serde_json::from_str(body) {
      Ok(v) => Ok(v),
      Err(e) => Err(SRCError::new(
          "Invalid json string",
          Some(e.to_string().as_ref()),
          false,
      )),
  }
}

struct Collector(Vec<u8>);

impl Handler for Collector {
    fn write(&mut self, data: &[u8]) -> Result<usize, WriteError> {
        self.0.extend_from_slice(data);
        Ok(data.len())
    }
}

#[derive(Debug)]
pub struct SRCError {
    error: String,
    side: Option<String>,
    retriable: bool,
    cached: bool,
}

impl SRCError {
  pub fn new(error: &str, cause: Option<&str>, retriable: bool) -> SRCError {
      let side = match cause {
          Some(v) => Some(v.to_owned()),
          None => None,
      };
      SRCError {
          error: error.to_owned(),
          side,
          retriable,
          cached: false,
      }
  }

  pub fn into_cache(self) -> SRCError {
      SRCError {
          error: self.error,
          side: self.side,
          retriable: self.retriable,
          cached: true,
      }
  }
}

impl Clone for SRCError {
  fn clone(&self) -> SRCError {
      let side = match &self.side {
          Some(v) => Some(String::from(v.deref())),
          None => None,
      };
      SRCError {
          error: String::from(self.error.deref()),
          side,
          retriable: self.retriable,
          cached: self.cached,
      }
  }
}
