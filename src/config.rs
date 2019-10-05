use clap::arg_enum;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "krusti")]
pub struct Config {
    #[structopt(short, long, possible_values = &Mode::variants(), case_insensitive = true)]
    pub mode: Mode,

    #[structopt(short, long)]
    pub brokers: String,

    #[structopt(short, long)]
    pub topic: String,

    #[structopt(short, long, possible_values = &Deserializer::variants(), case_insensitive = true, default_value="string")]
    pub key_deserializer: Deserializer,

    #[structopt(short, long, possible_values = &Deserializer::variants(), case_insensitive = true, default_value="string")]
    pub value_deserializer: Deserializer,

    #[structopt(short, long, default_value = "localhost:8081")]
    pub registry_url: String,
}

arg_enum! {
    #[derive(Debug)]
    pub enum Mode {
        Consume,
        Produce,
        Describe,
    }
}

arg_enum! {
    #[derive(Debug)]
    pub enum Deserializer {
        String,
        Avro,
    }
}
