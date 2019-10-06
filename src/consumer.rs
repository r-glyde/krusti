use crate::config::Deserializer;
use crate::deserializers::{avro_deserializer, string_deserializer};
use crate::kafka_helpers::Record;

use std::collections::HashMap;

use kafka::client::FetchOffset;
use kafka::consumer::Consumer;
use schema_registry_converter::Decoder;
use serde_json::Value as JsonValue;

pub fn run(
    brokers: String,
    topic: String,
    key_deserializer: Deserializer,
    value_deserializer: Deserializer,
    registry_url: String,
) {
    let mut consumer = Consumer::from_hosts(vec![brokers])
        .with_group("".to_owned())
        .with_fallback_offset(FetchOffset::Earliest)
        .with_topic(topic.to_owned())
        .create()
        .expect("Couldn't create a consumer!");

    let key_decoder = &mut Decoder::new(registry_url.to_owned());
    let value_decoder = &mut Decoder::new(registry_url.to_owned());

    let mut key_d: Box<dyn FnMut(Option<Vec<u8>>) -> JsonValue> = match key_deserializer {
        Deserializer::String => Box::new(string_deserializer),
        Deserializer::Avro => Box::new(|bytes| avro_deserializer(bytes, key_decoder)),
    };

    let mut value_d: Box<dyn FnMut(Option<Vec<u8>>) -> JsonValue> = match value_deserializer {
        Deserializer::String => Box::new(string_deserializer),
        Deserializer::Avro => Box::new(|bytes| avro_deserializer(bytes, value_decoder)),
    };

    let mut end_offsets = HashMap::new();
    let mut completed_partitions = HashMap::new();

    consumer
        .client_mut()
        .fetch_topic_offsets(topic.to_owned(), FetchOffset::Latest)
        .expect("Could not fetch offsets for topic")
        .iter()
        .for_each(|p| {
            end_offsets.insert(p.partition, p.offset);
            completed_partitions.insert(p.partition, false);
        });

    loop {
        consumer.poll().iter().for_each(|mss| {
            mss.iter().for_each(|ms| {
                let records: Vec<String> = ms
                    .messages()
                    .iter()
                    .map(|msg| {
                        if (msg.offset + 1) == *end_offsets.get(&ms.partition()).unwrap() {
                            eprintln!("reached end of partition [{}] at offset {}",
                                      ms.partition(), msg.offset);
                            completed_partitions.insert(ms.partition(), true);
                        }
                        let record = Record {
                            key: key_d(Some(msg.key.to_vec())),
                            //                        key: JsonValue::String("abc".to_owned()),
                            value: value_d(Some(msg.value.to_vec())),
                            //                        value: JsonValue::String("abc".to_owned()),
                            topic: ms.topic().to_owned(),
                            partition: ms.partition(),
                            offset: msg.offset,
                            timestamp: -1,
                        };
                        serde_json::to_string(&record).unwrap()
                    })
                    .collect();
                println!("{}", records.join("\n"));
            });
        });
        if completed_partitions.values().all(|&done| done) {
            break;
        }
    }
}
