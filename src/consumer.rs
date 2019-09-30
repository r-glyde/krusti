extern crate futures;
extern crate rdkafka;
extern crate tokio;

use std::collections::HashMap;
use std::i64;
use std::time::Duration;

use futures::stream::Stream;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::Message;
use rdkafka::topic_partition_list::Offset;
use rdkafka::TopicPartitionList;
use regex::Regex;
use schema_registry_converter::Decoder;
use serde_json::Value as JsonValue;
use tokio::runtime::current_thread;
use uuid::Uuid;

use crate::config::Deserializer;
use crate::deserializers::{avro_deserializer, string_deserializer};
use crate::kafka;

pub fn run_consumer(
    brokers: String,
    topic: String,
    key_deserializer: Deserializer,
    value_deserializer: Deserializer,
    registry_url: String,
) {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", &Uuid::new_v4().to_string())
        .set("bootstrap.servers", brokers.as_str())
        .set("enable.partition.eof", "true")
        .create()
        .expect("Consumer creation failed");

    let key_registry = &mut Decoder::new(registry_url.to_owned());
    let value_registry = &mut Decoder::new(registry_url.to_owned());

    let mut key_d: Box<dyn FnMut(Option<Vec<u8>>) -> JsonValue> = match key_deserializer {
        Deserializer::String => Box::new(|bytes| string_deserializer(bytes)),
        Deserializer::Avro => Box::new(|bytes| avro_deserializer(bytes, key_registry)),
    };

    let mut value_d: Box<dyn FnMut(Option<Vec<u8>>) -> JsonValue> = match value_deserializer {
        Deserializer::String => Box::new(|bytes| string_deserializer(bytes)),
        Deserializer::Avro => Box::new(|bytes| avro_deserializer(bytes, value_registry)),
    };

    let m = consumer
        .fetch_metadata(Some(&topic), Duration::from_secs(5))
        .unwrap();
    let t = m.topics().iter().find(|&t| t.name() == topic).unwrap();

    let mut beginning_tm = HashMap::new();
    let mut offsets = HashMap::new();
    let mut current_offsets = HashMap::new();

    let re: Regex = Regex::new(r"Partition EOF: (\d)+$").unwrap();

    t.partitions().iter().for_each(|p| {
        let (low, high) = consumer
            .fetch_watermarks(&topic, p.id(), Duration::from_secs(5))
            .unwrap();

        beginning_tm.insert((topic.to_owned(), p.id()), Offset::Beginning);
        offsets.insert(p.id(), kafka::Offsets { low, high });
    });
    consumer
        .assign(&TopicPartitionList::from_topic_map(&beginning_tm))
        .expect("Can't subscribe to specified partitions");

    let stream = consumer
        .start()
        .filter_map(|result| match result {
            Ok(msg) => {
                let current = current_offsets.entry(msg.partition()).or_insert(0);
                *current += 1;
                Some(msg)
            }
            Err(e) => {
                let pid: i32 = re.captures(e.to_string().as_str()).unwrap().get(1)?.as_str().parse().unwrap();
                let offset = current_offsets.get(&pid).unwrap();
                eprintln!("reached end of partition [{}] at offset {}", pid, offset);

                if offsets.iter().all(|(k, v)| {
                    let current = current_offsets.get(k).unwrap_or(&i64::max_value());
                    current == &v.high
                }) {
                    consumer.stop();
                }
                None
            }
        })
        .for_each(|msg| {
            let key = key_d(msg.key().map(|bytes| bytes.to_vec()));
            let value = value_d(msg.payload().map(|bytes| bytes.to_vec()));
            let record = kafka::Record::new(msg, key, value);

            println!("{}", serde_json::to_string(&record).unwrap());
            Ok(())
        });

    let mut io_thread = current_thread::Runtime::new().unwrap();
    let _ = io_thread.block_on(stream);
}
