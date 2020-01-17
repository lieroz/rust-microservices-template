#[macro_use]
extern crate log;

use actix_web::{middleware::Logger, web, App, HttpServer, HttpResponse};
use jsonwebtoken::{decode, encode, Header, Validation};
use listenfd::ListenFd;
use r2d2_redis::{r2d2, redis, RedisConnectionManager};
use serde::{Deserialize, Serialize};
use chrono::{Local, Duration};
use std::ops::DerefMut;

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    login: String,
    password: String,
    #[serde(default)]
    exp: usize,
}

impl PartialEq for Claims {
    fn eq(&self, other: &Self) -> bool {
        self.login == other.login && self.password == other.password
    }
}

static SECRET: &str = "secret";
static DURATION_DELTA: i64 = 30;

fn generate_token(claims: &mut Claims) -> String {
    claims.exp = (Local::now() + Duration::minutes(DURATION_DELTA))
        .timestamp() as usize;
    let token = encode(&Header::default(), &claims, SECRET.as_ref())
        .expect("Can't generate token");
    token
}

async fn auth(
    claims: web::Json<Claims>,
    pool: web::Data<r2d2::Pool<RedisConnectionManager>>
) -> HttpResponse {
    let mut claims = claims.into_inner();

    let mut conn = pool.get().expect("Can't get connection from pool");
    let token = match redis::cmd("GET").arg(&claims.login).query(conn.deref_mut()) {
        Ok(value) => match value {
            redis::Value::Nil => {
                let token = generate_token(&mut claims);
                let _ = redis::cmd("SET").arg(&[&claims.login, &token])
                    .query::<String>(conn.deref_mut());
                token
            }
            redis::Value::Data(data) => {
                let token = String::from_utf8(data.to_vec())
                    .expect("Can't deserialize to String");
                match decode::<Claims>(&token, SECRET.as_ref(), &Validation::default()) {
                    Ok(cl) => {
                        if cl.claims == claims {
                            token
                        } else {
                            error!(
                                "Passed password for user '{}' doesn't match with one in database",
                                claims.login
                            );
                            return HttpResponse::BadRequest().finish();
                        }
                    }
                    Err(e) => match e.kind() {
                        jsonwebtoken::errors::ErrorKind::ExpiredSignature => {
                            let token = generate_token(&mut claims);
                            let _ = redis::cmd("SET").arg(&[&claims.login, &token])
                                .query::<String>(conn.deref_mut());
                            token
                        }
                        _ => {
                            error!("{}", e);
                            return HttpResponse::InternalServerError().finish();
                        }
                    }
                }
            },
            _ => {
                return HttpResponse::InternalServerError().finish();
            }
        }
        Err(e) => {
            error!("{}", e);
            return HttpResponse::InternalServerError().finish();
        }
    };

    HttpResponse::Ok().json(token)
}

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "debug");
    env_logger::init();

    let manager = RedisConnectionManager::new("redis://172.17.0.4:6379")
        .expect("Failed to connect to redis server");
    let pool = r2d2::Pool::builder()
        .build(manager)
        .expect("Failed to create pool");

    let mut listen_fd = ListenFd::from_env();
    let mut server = HttpServer::new(move || {
        App::new()
            .data(pool.clone())
            .wrap(Logger::new(
                "ip: %a, date: %t, response code: %s, response size: %b (bytes), duration: %D (ms)",
            ))
            .service(web::resource("/auth").route(web::post().to(auth)))
    });

    server = if let Some(l) = listen_fd.take_tcp_listener(0)? {
        server.listen(l)?
    } else {
        server.workers(4).bind(format!("0.0.0.0:{}", 3000))?
    };

    server.run().await
}
