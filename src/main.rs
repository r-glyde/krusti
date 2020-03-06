extern crate krusti;

use krusti::config::Config;
use krusti::consumer;
use structopt::StructOpt;

#[tokio::main]
async fn main() {
    let Config {
        brokers,
        topic,
        key_deserializer,
        value_deserializer,
        registry_url,
    } = Config::from_args();

    consumer::run_consumer(
            brokers,
            topic,
            key_deserializer,
            value_deserializer,
            registry_url,
        ).await

}
