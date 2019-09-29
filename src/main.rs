extern crate futures;
extern crate rdkafka;
extern crate regex;
extern crate tokio;

use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::i64;
use std::str;
use std::time::Duration;

use futures::stream::Stream;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::Consumer;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::message::{BorrowedMessage, Message};
use rdkafka::topic_partition_list::Offset;
use rdkafka::TopicPartitionList;
use serde::Serialize;
use tokio::runtime::current_thread;
use uuid::Uuid;
use regex::Regex;

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

fn main() {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", &Uuid::new_v4().to_string())
        .set("bootstrap.servers", "localhost:9093")
        .set("enable.partition.eof", "true")
        .create()
        .expect("Consumer creation failed");

    let topic = "topic-1";

    let m = consumer
        .fetch_metadata(Some(&topic), Duration::from_secs(5))
        .unwrap();
    let t = m.topics().iter().find(|t| t.name() == topic).unwrap();

    let beginning_tm = HashMap::from(
        t.partitions()
            .iter()
            .map(|p| ((topic.to_owned(), p.id()), Offset::Beginning))
            .collect(),
    );

    let offsets: HashMap<i32, Offsets, RandomState> = HashMap::from(
        t.partitions()
            .iter()
            .map(|p| {
                let (low, high) = consumer
                    .fetch_watermarks(&topic, p.id(), Duration::from_secs(5))
                    .unwrap();
                (p.id(), Offsets { low, high })
            })
            .collect(),
    );

    let mut bars: HashMap<i32, ProgressBar> = HashMap::new();
    let m = MultiProgress::new();
    let sty = ProgressStyle::default_bar()
        .template("[{bar:60.cyan/blue}] {pos}/{len} | {msg}")
        .progress_chars("#>-");

    offsets.iter().for_each(|(&p, o)|{
        let pb = m.add(ProgressBar::new((o.high - o.low) as u64));
        pb.set_style(sty.clone());
        pb.set_message(&format!("partition [{}]", p));
        bars.insert(p,pb);
    });

    consumer
        .assign(&TopicPartitionList::from_topic_map(&beginning_tm))
        .expect("Can't subscribe to specified partitions");

    let mut current_offsets: HashMap<i32, i64, RandomState> = HashMap::new();
    let re: Regex = Regex::new(r"Partition EOF: (\d)+$").unwrap();

    let stream = consumer
        .start()
        .filter_map(|result| match result {
            Ok(msg) => {
                let current = current_offsets.entry(msg.partition()).or_insert(0);
                let pb = bars.get(&msg.partition()).unwrap();
//                pb.inc(1);
                *current += 1;
                Some(msg)
            }
            Err(kafka_error) => {
                let text = kafka_error.to_string();
                let pid: i32 = re.captures(&text).unwrap().get(1)?.as_str().parse().unwrap();
                let offset = current_offsets.get(&pid).unwrap();

                let pb = bars.get(&pid).unwrap();
                pb.finish_with_message(&format!("end of partition [{}] at offset {}", pid, offset));

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
            let record = Record::new(msg);
            println!("{}", serde_json::to_string(&record).unwrap());
            Ok(())
        });

    let mut io_thread = current_thread::Runtime::new().unwrap();
    let _ = io_thread.block_on(stream);
    m.join().unwrap();
}

#[derive(Debug)]
struct Offsets {
    low: i64,
    high: i64,
}

#[derive(Serialize)]
struct Record {
    key: Option<serde_json::Value>,
    value: Option<serde_json::Value>,
    topic: String,
    partition: i32,
    offset: i64,
}

impl Record {
    fn new(msg: BorrowedMessage) -> Record {
        let key = msg.key().map(|key| match str::from_utf8(key) {
            Ok(k) => serde_json::from_str(&k).unwrap(),
            Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
        });

        let value = msg.payload().map(|value| match str::from_utf8(value) {
            Ok(v) => serde_json::from_str(&v).unwrap(),
            Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
        });

        Record {
            key,
            value,
            topic: msg.topic().to_owned(),
            partition: msg.partition(),
            offset: msg.offset(),
        }
    }
}
