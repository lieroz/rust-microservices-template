use actix_web::{middleware::Logger, App, HttpServer};

mod appconfig;
mod handlers;
mod kafka_consumer;

fn main() {
    std::env::set_var("RUST_LOG", "actix_web=debug");
    env_logger::init();

    let handler = std::thread::spawn(kafka_consumer::consume_and_process);
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
        server.workers(4).bind("0.0.0.0:8081").unwrap()
    };

    server.start();
    let _ = sys.run();
    handler.join().unwrap();
}
