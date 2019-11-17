use actix_web::{middleware::Logger, App, HttpServer};
use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureProducer;
use serde::Deserialize;

mod appconfig;
mod handlers;

// TODO: maybe pass errors to highest layer?
#[derive(Clone, Deserialize)]
pub struct Config {
    server_port: String,
    kafka_brokers: String,
    kafka_message_timeout: String,
    order_service_kafka_topic: String,
    billing_service_kafka_topic: String,
}

fn read_config(config_file_path: &str) -> Result<String, std::io::Error> {
    std::fs::read_to_string(config_file_path)
}

fn parse_config(config: &str) -> Result<Config, toml::de::Error> {
    toml::from_str(config)
}

fn read_and_parse_config(config_file_path: &str) -> Option<Config> {
    if let Ok(config) = read_config(config_file_path) {
        if let Ok(config) = parse_config(&config) {
            return Some(config);
        }
    }
    None
}

fn main() {
    std::env::set_var("RUST_LOG", "actix_web=debug");
    env_logger::init();

    let matches = clap::App::new("rsoi gateway")
        .arg(
            clap::Arg::with_name("config")
                .short("c")
                .long("config")
                .value_name("FILE")
                .help("Sets a custom config file")
                .takes_value(true),
        )
        .get_matches();

    if let Some(config) = matches.value_of("config") {
        if let Some(config) = read_and_parse_config(config) {
            let sys = actix_rt::System::new("gateway");
            let producer: FutureProducer = ClientConfig::new()
                .set("bootstrap.servers", &config.kafka_brokers)
                .set("message.timeout.ms", &config.kafka_message_timeout)
                .create()
                .expect("Producer creation error");
            let bind_addr = format!("0.0.0.0:{}", config.server_port);

            let mut listen_fd = listenfd::ListenFd::from_env();
            let mut server = HttpServer::new(move || {
                App::new()
            .configure(appconfig::config_app)
            .data(producer.clone())
            .data(config.clone())
            .wrap(Logger::new(
                "ip: %a, date: %t, response code: %s, response size: %b (bytes), duration: %D (ms)",
            ))
            });

            server = if let Some(l) = listen_fd.take_tcp_listener(0).unwrap() {
                server.listen(l).unwrap()
            } else {
                server.workers(4).bind(bind_addr).unwrap()
            };

            server.start();
            let _ = sys.run();
        }
    }
}
