extern crate krusti;

use krusti::config::{Config, Mode};
use krusti::consumer;
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
        Mode::Produce => println!("producing not supported yet..."),
        Mode::Describe => println!("describing not supported yet..."),
    }
}
