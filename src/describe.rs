use kafka::client::{FetchOffset, KafkaClient};

pub fn run(brokers: String, topic: String) {

    let mut client = KafkaClient::new(vec![brokers]);

    client.load_metadata_all().unwrap();

}

fn list_topics(client: &KafkaClient) {
    client.topics().iter().for_each(|topic| {
        println!("{}:", topic.name().to_owned());
        topic.partitions().iter().for_each(|p| {
            let broker_info = match p.leader() {
                Some(b) => format!("{{ node_id: {}, broker_host: {} }}", b.id(), b.host()),
                None => "unavailable".to_owned(),
            };
            println!("  {:3} - leader: {}", p.id(), broker_info);
        })
    });
}

fn topic_offsets(client: &mut KafkaClient, topic: &str) {
    let beginning = client.fetch_topic_offsets(topic.to_owned(), FetchOffset::Earliest);
    let end = client.fetch_topic_offsets(topic.to_owned(), FetchOffset::Latest);

    match (beginning, end) {
        (Ok(beginning), Ok(end)) => {
            println!("Offsets for {}:", topic);
            beginning.iter().zip(end.iter()).for_each(|(b, e)| {
                println!("  partition [{}]: {} -> {}", b.partition, b.offset, e.offset);
            });
        },
        _ => eprintln!("Could not find offsets for: {}", topic)
    }
}
