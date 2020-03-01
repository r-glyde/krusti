extern crate krusti;

use krusti::config::{Config, Mode};
use krusti::consumer;
use structopt::StructOpt;

#[tokio::main]
async fn main() {
    let Config {
        mode,
        brokers,
        topic,
        key_deserializer,
        value_deserializer,
        registry_url,
    } = Config::from_args();

    match mode {
        Mode::Consumer => consumer::run_consumer(
            brokers,
            topic,
            key_deserializer,
            value_deserializer,
            registry_url,
        ).await,
        Mode::Producer => eprintln!("producing not supported yet..."),
    }
}
