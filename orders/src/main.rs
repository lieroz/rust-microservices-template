#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;

use actix_web::{middleware::Logger, App, HttpServer};
use r2d2_redis::{r2d2, RedisConnectionManager};
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use serde::Deserialize;
use signal_hook::{iterator::Signals, SIGINT, SIGQUIT, SIGTERM};
use std::sync::Arc;

mod api;
mod appconfig;
mod db;
mod kafka_consumer;
mod validation_schema;

#[derive(Deserialize)]
struct ServerOptions {
    port: usize,
    workers: usize,
    kafka_workers: usize,
    log_level: String,
    redis_connection_string: String,
}

#[derive(Clone, Deserialize)]
pub struct KafkaConsumerOptions {
    group_id: String,
    bootstrap_servers: String,
    enable_partition_eof: String,
    session_timeout_ms: String,
    enable_auto_commit: String,
}

#[derive(Clone, Deserialize)]
pub struct KafkaTopics {
    orders_service_topic: String,
}

#[derive(Deserialize)]
struct Config {
    server: ServerOptions,
    kafka_consumer: KafkaConsumerOptions,
    kafka_topics: KafkaTopics,
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

            let manager =
                RedisConnectionManager::new(&config.server.redis_connection_string[..]).unwrap();
            let pool = r2d2::Pool::builder().build(manager).unwrap();

            let mut handlers = vec![];
            let mut consumers: Vec<Arc<StreamConsumer<kafka_consumer::OrdersContext>>> = vec![];

            for _ in 0..config.server.kafka_workers {
                let consumer: Arc<StreamConsumer<kafka_consumer::OrdersContext>> = Arc::new(
                    ClientConfig::new()
                        .set("group.id", &config.kafka_consumer.group_id)
                        .set(
                            "bootstrap.servers",
                            &config.kafka_consumer.bootstrap_servers,
                        )
                        .set(
                            "enable.partition.eof",
                            &config.kafka_consumer.enable_partition_eof,
                        )
                        .set(
                            "session.timeout.ms",
                            &config.kafka_consumer.session_timeout_ms,
                        )
                        .set(
                            "enable.auto.commit",
                            &config.kafka_consumer.enable_auto_commit,
                        )
                        .set_log_level(RDKafkaLogLevel::Debug)
                        .create_with_context(kafka_consumer::OrdersContext)
                        .expect("Consumer creation failed"),
                );
                consumers.push(Arc::clone(&consumer));

                let kafka_topics = config.kafka_topics.clone();
                let pool = pool.clone();

                handlers.push(std::thread::spawn(move || {
                    kafka_consumer::consume_and_process(kafka_topics, Arc::clone(&consumer), pool)
                }));
            }

            let signals = Signals::new(&[SIGINT, SIGTERM, SIGQUIT]).unwrap();
            let signal_handler = std::thread::spawn(move || {
                for _ in signals.forever() {
                    for consumer in consumers.iter() {
                        consumer.stop();
                    }

                    break;
                }
            });

            let sys = actix_rt::System::new("orders");

            let mut listen_fd = listenfd::ListenFd::from_env();
            let mut server = HttpServer::new(move || {
                App::new()
                    .configure(appconfig::config_app)
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

            signal_handler.join().unwrap();

            for handler in handlers {
                handler.join().unwrap();
            }
        }
    }
}
