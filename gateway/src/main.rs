#[macro_use]
extern crate log;

use actix_web::{middleware::Logger, App, HttpServer};
use r2d2_redis::{r2d2, RedisConnectionManager};
use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureProducer;
use serde::Deserialize;

mod api;
mod appconfig;

#[derive(Deserialize)]
struct ServerOptions {
    port: usize,
    workers: usize,
    log_level: String,
    redis_connection_string: String,
}

#[derive(Deserialize)]
struct KafkaProducerOptions {
    bootstrap_servers: String,
    message_timeout_ms: String,
}

#[derive(Clone, Deserialize)]
pub struct KafkaTopics {
    orders_service_topic: String,
    billing_service_topic: String,
}

#[derive(Clone, Deserialize)]
pub struct ServicesParams {
    orders_service_addr: String,
    warehouse_service_addr: String,
}

#[derive(Deserialize)]
struct Config {
    server: ServerOptions,
    kafka_producer: KafkaProducerOptions,
    kafka_topics: KafkaTopics,
    services: ServicesParams,
}

fn read_config(config_file_path: &str) -> Result<String, std::io::Error> {
    std::fs::read_to_string(config_file_path)
}

fn parse_config(config: &str) -> Result<Config, toml::de::Error> {
    toml::from_str(config)
}

// TODO: maybe implement error handling?
fn read_and_parse_config(config_file_path: &str) -> Option<Config> {
    if let Ok(config) = read_config(config_file_path) {
        if let Ok(config) = parse_config(&config) {
            return Some(config);
        }
    }
    None
}

fn main() {
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
            std::env::set_var("RUST_LOG", &config.server.log_level);
            env_logger::init();

            let sys = actix_rt::System::new("gateway");
            let producer: FutureProducer = ClientConfig::new()
                .set(
                    "bootstrap.servers",
                    &config.kafka_producer.bootstrap_servers,
                )
                .set(
                    "message.timeout.ms",
                    &config.kafka_producer.message_timeout_ms,
                )
                .create()
                .expect("Producer creation error");
            let kafka_topics = config.kafka_topics.clone();
            let services_params = config.services.clone();

            let manager =
                RedisConnectionManager::new(&config.server.redis_connection_string[..]).unwrap();
            let pool = r2d2::Pool::builder().build(manager).unwrap();

            let mut listen_fd = listenfd::ListenFd::from_env();
            let mut server = HttpServer::new(move || {
                App::new()
                    .configure(appconfig::config_app)
                    .data(producer.clone())
                    .data(kafka_topics.clone())
                    .data(services_params.clone())
                    .data(pool.clone())
                    .wrap(Logger::new(
                        "ip: %a, date: %t, response code: %s, response size: %b (bytes), duration: %D (ms)",
                    ))
            });

            server = if let Some(l) = listen_fd.take_tcp_listener(0).unwrap() {
                server.listen(l).unwrap()
            } else {
                server
                    .workers(config.server.workers)
                    .bind(format!("0.0.0.0:{}", config.server.port))
                    .unwrap()
            };

            server.start();
            let _ = sys.run();
        }
    }
}
