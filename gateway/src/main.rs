mod appconfig;
mod handlers;

use actix_web::{middleware::Logger, App, HttpServer};

fn main() {
    std::env::set_var("RUST_LOG", "actix_web=debug");

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
        server.workers(4).bind("0.0.0.0:8080").unwrap()
    };

    server.run().unwrap();
}
