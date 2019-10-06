extern crate krusti;

use krusti::config::{Config, Mode};
use krusti::{consumer, describe};
use structopt::StructOpt;

fn main() {
    let Config {
        mode,
        brokers,
        topic,
        key_deserializer,
        value_deserializer,
        registry_url,
    } = Config::from_args();

    match mode {
        Mode::Consume => consumer::run(
            brokers,
            topic,
            key_deserializer,
            value_deserializer,
            registry_url,
        ),
        Mode::Describe => describe::run(brokers, topic),
        Mode::Produce => println!("producing not supported yet..."),
    }
}
