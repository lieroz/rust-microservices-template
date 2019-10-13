mod appconfig;
mod handlers;

use actix_web::{middleware::Logger, App, HttpServer};
use r2d2_redis::{r2d2, RedisConnectionManager};

fn main() {
    std::env::set_var("RUST_LOG", "actix_web=info");
    env_logger::init();

    let manager = RedisConnectionManager::new("redis://127.0.0.1:6379").unwrap();
    let pool = r2d2::Pool::builder().build(manager).unwrap();

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
        server.workers(4).bind("0.0.0.0:8080").unwrap()
    };

    server.run().unwrap();
}
