use actix_web::{middleware::Logger, App, HttpServer};
use serde::Deserialize;

mod appconfig;
mod handlers;
mod kafka_consumer;

#[derive(Deserialize)]
struct ServerOptions {
    port: usize,
    workers: usize,
    kafka_workers: usize,
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
            let mut handlers = vec![];

            for _ in 0..config.server.kafka_workers {
                let kafka_consumer_options = config.kafka_consumer.clone();
                let kafka_topics = config.kafka_topics.clone();

                handlers.push(std::thread::spawn(move || {
                    kafka_consumer::consume_and_process(kafka_consumer_options, kafka_topics)
                }));
            }

            let sys = actix_rt::System::new("orders");

            let mut listen_fd = listenfd::ListenFd::from_env();
            let mut server = HttpServer::new(|| {
                App::new()
                    .configure(appconfig::config_app)
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

            for handler in handlers {
                handler.join().unwrap();
            }
        }
    }
}
