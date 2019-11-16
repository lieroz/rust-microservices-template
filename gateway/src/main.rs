use actix_web::{middleware::Logger, App, HttpServer};
use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureProducer;

mod appconfig;
mod handlers;

fn main() {
    std::env::set_var("RUST_LOG", "actix_web=debug");
    env_logger::init();

    let sys = actix_rt::System::new("gateway");
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    let mut listen_fd = listenfd::ListenFd::from_env();
    let mut server = HttpServer::new(move || {
        App::new()
            .configure(appconfig::config_app)
            .data(producer.clone())
            .wrap(Logger::new(
                "ip: %a, date: %t, response code: %s, response size: %b (bytes), duration: %D (ms)",
            ))
    });

    server = if let Some(l) = listen_fd.take_tcp_listener(0).unwrap() {
        server.listen(l).unwrap()
    } else {
        server.workers(4).bind("0.0.0.0:8080").unwrap()
    };

    server.start();
    let _ = sys.run();
}
