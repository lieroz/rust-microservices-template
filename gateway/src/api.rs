use crate::{KafkaTopics, ServicesParams};
use actix_web::http::StatusCode;
use actix_web::{web, HttpRequest, HttpResponse};
use crypto::digest::Digest;
use crypto::sha2::Sha256;
use futures::*;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Good {
    #[serde(alias = "good_id")]
    id: u64,
    count: u64,
    #[serde(default)]
    naming: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Order {
    status: String,
    goods: Vec<Good>,
}

fn liveness_probe(host: &str, path: &str, f: &dyn Fn(&str, &str) -> HttpResponse) -> HttpResponse {
    match reqwest::get(&format!("http://{}/probe/liveness", host)) {
        Ok(res) => {
            if res.status().is_success() {
                f(host, path)
            } else {
                HttpResponse::build(res.status()).finish()
            }
        }
        Err(e) => {
            error!("Error: {}", e);
            HttpResponse::InternalServerError().finish()
        }
    }
}

fn liveness_probe_v2(
    host: &str,
    path: &str,
    f: &mut dyn FnMut(&str, &str) -> StatusCode,
) -> StatusCode {
    match reqwest::get(&format!("http://{}/probe/liveness", host)) {
        Ok(res) => {
            if res.status().is_success() {
                f(host, path)
            } else {
                res.status()
            }
        }
        Err(e) => {
            error!("Error: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

pub fn get_orders(req: HttpRequest, services_params: web::Data<ServicesParams>) -> HttpResponse {
    liveness_probe(
        &services_params.orders_service_addr,
        &req.path(),
        &|host, path| match reqwest::get(&format!("http://{}{}", host, path)) {
            Ok(mut res) => match res.text() {
                Ok(text) => HttpResponse::build(res.status())
                    .content_type("application/json")
                    .body(text),
                Err(e) => {
                    error!("Error: {}", e);
                    HttpResponse::InternalServerError().finish()
                }
            },
            Err(e) => {
                error!("Error: {}", e);
                HttpResponse::InternalServerError().finish()
            }
        },
    )
}

pub fn create_order(
    bytes: web::Bytes,
    user_id: web::Path<String>,
    producer: web::Data<FutureProducer>,
    kafka_topics: web::Data<KafkaTopics>,
) -> HttpResponse {
    let mut hasher = Sha256::new();
    hasher.input(user_id.as_bytes());
    hasher.input(bytes.as_ref());
    let key = hasher.result_str();

    let payload = match std::str::from_utf8(bytes.as_ref()) {
        Ok(s) => s,
        Err(e) => {
            error!("{}:Couldn't deserialize payload: {}", line!(), e);
            return HttpResponse::BadRequest().finish();
        }
    };

    let result = producer
        .send(
            FutureRecord::to(&kafka_topics.orders_service_topic)
                .key(&key)
                .payload(payload)
                .headers(
                    OwnedHeaders::new()
                        .add("operation", "create")
                        .add("user_id", user_id.as_ref()),
                ),
            0,
        )
        .wait();

    match result {
        Ok(Ok(delivery)) => {
            info!(
                "Message sent to kafka: partition: {}, offset: {}",
                delivery.0, delivery.1
            );
            HttpResponse::Created().finish()
        }
        Ok(Err((e, msg))) => {
            error!(
                "{}:Error occured while sending message to kafka: error: {}, message: {:?}",
                line!(),
                e,
                msg
            );
            HttpResponse::BadRequest().finish()
        }
        Err(_) => HttpResponse::InternalServerError().finish(),
    }
}

pub fn get_order(req: HttpRequest, services_params: web::Data<ServicesParams>) -> HttpResponse {
    liveness_probe(
        &services_params.orders_service_addr,
        &req.path(),
        &|host, path| {
            let mut res = match reqwest::get(&format!("http://{}{}", host, path)) {
                Ok(res) => res,
                Err(e) => {
                    error!("Error: {}", e);
                    return HttpResponse::InternalServerError().finish();
                }
            };

            if res.status().is_success() {
                let mut order: Order = match res.json() {
                    Ok(x) => x,
                    Err(e) => {
                        error!("Error: {}", e);
                        return HttpResponse::InternalServerError().finish();
                    }
                };

                for good in &mut order.goods {
                    liveness_probe_v2(
                        &services_params.warehouse_service_addr,
                        &good.id.to_string(),
                        &mut |host, path| match reqwest::get(&format!(
                            "http://{}/goods/{}",
                            host, path
                        )) {
                            Ok(mut res) => {
                                let g: Good = match res.json() {
                                    Ok(g) => g,
                                    Err(e) => {
                                        error!("Error: {}", e);
                                        return StatusCode::INTERNAL_SERVER_ERROR;
                                    }
                                };

                                good.naming = g.naming;
                                StatusCode::OK
                            }
                            Err(e) => {
                                error!("Error: {}", e);
                                StatusCode::INTERNAL_SERVER_ERROR
                            }
                        },
                    );
                }

                HttpResponse::Ok().json(order)
            } else {
                HttpResponse::build(res.status()).finish()
            }
        },
    )
}

pub fn update_order(
    bytes: web::Bytes,
    params: web::Path<(String, String)>,
    producer: web::Data<FutureProducer>,
    kafka_topics: web::Data<KafkaTopics>,
) -> HttpResponse {
    let mut hasher = Sha256::new();
    hasher.input(params.0.as_bytes());
    hasher.input(params.1.as_bytes());
    hasher.input(bytes.as_ref());
    let key = hasher.result_str();

    let payload = match std::str::from_utf8(bytes.as_ref()) {
        Ok(s) => s,
        Err(e) => {
            error!("{}:Couldn't deserialize payload: {}", line!(), e);
            return HttpResponse::BadRequest().finish();
        }
    };

    let result = producer
        .send(
            FutureRecord::to(&kafka_topics.orders_service_topic)
                .key(&key[..])
                .payload(payload)
                .headers(
                    OwnedHeaders::new()
                        .add("operation", "update")
                        .add("user_id", &params.0)
                        .add("order_id", &params.1),
                ),
            0,
        )
        .wait();

    match result {
        Ok(Ok(delivery)) => {
            info!(
                "Message sent to kafka: partition: {}, offset: {}",
                delivery.0, delivery.1
            );
            HttpResponse::Ok().finish()
        }
        Ok(Err((e, msg))) => {
            error!(
                "{}:Error occured while sending message to kafka: error: {}, message: {:?}",
                line!(),
                e,
                msg
            );
            HttpResponse::BadRequest().finish()
        }
        Err(_) => HttpResponse::InternalServerError().finish(),
    }
}

pub fn delete_order(
    bytes: web::Bytes,
    params: web::Path<(String, String)>,
    producer: web::Data<FutureProducer>,
    kafka_topics: web::Data<KafkaTopics>,
) -> HttpResponse {
    let mut hasher = Sha256::new();
    hasher.input(params.0.as_bytes());
    hasher.input(params.1.as_bytes());
    hasher.input(bytes.as_ref());
    let key = hasher.result_str();

    let payload = match std::str::from_utf8(bytes.as_ref()) {
        Ok(s) => s,
        Err(e) => {
            error!("{}:Couldn't deserialize payload: {}", line!(), e);
            return HttpResponse::BadRequest().finish();
        }
    };

    let result = producer
        .send(
            FutureRecord::to(&kafka_topics.orders_service_topic)
                .key(&key[..])
                .payload(payload)
                .headers(
                    OwnedHeaders::new()
                        .add("operation", "delete")
                        .add("user_id", &params.0)
                        .add("order_id", &params.1),
                ),
            0,
        )
        .wait();

    match result {
        Ok(Ok(delivery)) => {
            info!(
                "Message sent to kafka: partition: {}, offset: {}",
                delivery.0, delivery.1
            );
            HttpResponse::Ok().finish()
        }
        Ok(Err((e, msg))) => {
            error!(
                "{}:Error occured while sending message to kafka: error: {}, message: {:?}",
                line!(),
                e,
                msg
            );
            HttpResponse::BadRequest().finish()
        }
        Err(_) => HttpResponse::InternalServerError().finish(),
    }
}

pub fn make_billing(
    bytes: web::Bytes,
    params: web::Path<(String, String)>,
    producer: web::Data<FutureProducer>,
    kafka_topics: web::Data<KafkaTopics>,
) -> HttpResponse {
    let mut hasher = Sha256::new();
    hasher.input(params.0.as_bytes());
    hasher.input(params.1.as_bytes());
    hasher.input(bytes.as_ref());
    let key = hasher.result_str();

    let payload = match std::str::from_utf8(bytes.as_ref()) {
        Ok(s) => s,
        Err(e) => {
            error!("{}:Couldn't deserialize payload: {}", line!(), e);
            return HttpResponse::BadRequest().finish();
        }
    };

    let result = producer
        .send(
            FutureRecord::to(&kafka_topics.billing_service_topic)
                .key(&key[..])
                .payload(payload)
                .headers(
                    OwnedHeaders::new()
                        .add("user_id", &params.0)
                        .add("order_id", &params.1),
                ),
            0,
        )
        .wait();

    match result {
        Ok(Ok(delivery)) => {
            info!(
                "Message sent to kafka: partition: {}, offset: {}",
                delivery.0, delivery.1
            );
            HttpResponse::Created().finish()
        }
        Ok(Err((error, message))) => {
            error!(
                "{}:Error occured while sending message to kafka: error: {}, message: {:?}",
                line!(),
                error,
                message
            );
            HttpResponse::BadRequest().finish()
        }
        Err(_) => HttpResponse::InternalServerError().finish(),
    }
}

pub fn get_goods(req: HttpRequest, services_params: web::Data<ServicesParams>) -> HttpResponse {
    liveness_probe(
        &services_params.warehouse_service_addr,
        &req.path(),
        &|host, path| match reqwest::get(&format!("http://{}{}", host, path)) {
            Ok(mut res) => match res.text() {
                Ok(text) => HttpResponse::build(res.status())
                    .content_type("application/json")
                    .body(text),
                Err(e) => {
                    error!("Error: {}", e);
                    HttpResponse::InternalServerError().finish()
                }
            },
            Err(e) => {
                error!("Error: {}", e);
                HttpResponse::InternalServerError().finish()
            }
        },
    )
}

pub fn get_good(req: HttpRequest, services_params: web::Data<ServicesParams>) -> HttpResponse {
    liveness_probe(
        &services_params.warehouse_service_addr,
        &req.path(),
        &|host, path| match reqwest::get(&format!("http://{}{}", host, path)) {
            Ok(mut res) => match res.text() {
                Ok(text) => HttpResponse::build(res.status())
                    .content_type("application/json")
                    .body(text),
                Err(e) => {
                    error!("Error: {}", e);
                    HttpResponse::InternalServerError().finish()
                }
            },
            Err(e) => {
                error!("Error: {}", e);
                HttpResponse::InternalServerError().finish()
            }
        },
    )
}
