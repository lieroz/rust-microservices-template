use actix_web::{web, HttpResponse};
use futures::*;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;

static ORDERS_TOPIC: &str = "orders";

pub fn get_orders() -> HttpResponse {
    // send to orders service api method
    HttpResponse::Ok().finish()
}

pub fn create_order(
    bytes: web::Bytes,
    user_id: web::Path<(String)>,
    producer: web::Data<FutureProducer>,
) -> HttpResponse {
    let key = bytes.hash(&mut DefaultHasher::new());

    let result = producer
        .send(
            FutureRecord::to(ORDERS_TOPIC)
                .key(&key)
                .payload(&String::from_utf8(bytes.to_vec()).unwrap())
                .headers(OwnedHeaders::new().add("user_id", user_id.as_ref())),
            0,
        )
        .wait();

    match result {
        Ok(Ok(delivery)) => HttpResponse::Created().json(format!(
            r#"{{"partition": "{}", "offset: "{}"}}"#,
            delivery.0, delivery.1
        )),
        Ok(Err((error, message))) => HttpResponse::BadRequest().json(format!(
            r#"{{"error": "{}", "message": "{:?}"}}"#,
            error, message
        )),
        Err(_) => HttpResponse::InternalServerError().finish(),
    }
}

pub fn get_order_detailed() -> HttpResponse {
    // send to orders service api method
    HttpResponse::Ok().finish()
}

pub fn update_order(
    bytes: web::Bytes,
    params: web::Path<(String, String)>,
    producer: web::Data<FutureProducer>,
) -> HttpResponse {
    let key = bytes.hash(&mut DefaultHasher::new());

    let result = producer
        .send(
            FutureRecord::to(ORDERS_TOPIC)
                .key(&key)
                .payload(&String::from_utf8(bytes.to_vec()).unwrap())
                .headers(
                    OwnedHeaders::new()
                        .add("user_id", &params.0)
                        .add("id", &params.1),
                ),
            0,
        )
        .wait();

    match result {
        Ok(Ok(delivery)) => HttpResponse::Created().json(format!(
            r#"{{"partition": "{}", "offset: "{}"}}"#,
            delivery.0, delivery.1
        )),
        Ok(Err((error, message))) => HttpResponse::BadRequest().json(format!(
            r#"{{"error": "{}", "message": "{:?}"}}"#,
            error, message
        )),
        Err(_) => HttpResponse::InternalServerError().finish(),
    }
}

pub fn add_good_to_order(
    bytes: web::Bytes,
    params: web::Path<(String, String, String)>,
    producer: web::Data<FutureProducer>,
) -> HttpResponse {
    let key = bytes.hash(&mut DefaultHasher::new());

    let result = producer
        .send(
            FutureRecord::to(ORDERS_TOPIC)
                .key(&key)
                .payload(&String::from_utf8(bytes.to_vec()).unwrap())
                .headers(
                    OwnedHeaders::new()
                        .add("user_id", &params.0)
                        .add("order_id", &params.1)
                        .add("good_id", &params.2),
                ),
            0,
        )
        .wait();

    match result {
        Ok(Ok(delivery)) => HttpResponse::Created().json(format!(
            r#"{{"partition": "{}", "offset: "{}"}}"#,
            delivery.0, delivery.1
        )),
        Ok(Err((error, message))) => HttpResponse::BadRequest().json(format!(
            r#"{{"error": "{}", "message": "{:?}"}}"#,
            error, message
        )),
        Err(_) => HttpResponse::InternalServerError().finish(),
    }
}

pub fn delete_good_from_order(
    bytes: web::Bytes,
    params: web::Path<(String, String, String)>,
    producer: web::Data<FutureProducer>,
) -> HttpResponse {
    let key = bytes.hash(&mut DefaultHasher::new());

    let result = producer
        .send(
            FutureRecord::to(ORDERS_TOPIC)
                .key(&key)
                .payload(&String::from_utf8(bytes.to_vec()).unwrap())
                .headers(
                    OwnedHeaders::new()
                        .add("user_id", &params.0)
                        .add("order_id", &params.1)
                        .add("good_id", &params.2),
                ),
            0,
        )
        .wait();

    match result {
        Ok(Ok(delivery)) => HttpResponse::Created().json(format!(
            r#"{{"partition": "{}", "offset: "{}"}}"#,
            delivery.0, delivery.1
        )),
        Ok(Err((error, message))) => HttpResponse::BadRequest().json(format!(
            r#"{{"error": "{}", "message": "{:?}"}}"#,
            error, message
        )),
        Err(_) => HttpResponse::InternalServerError().finish(),
    }
}
