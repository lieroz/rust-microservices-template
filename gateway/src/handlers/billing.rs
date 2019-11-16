use actix_web::{web, HttpResponse};
use futures::*;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;

static BILLINGS_TOPIC: &str = "billings";

pub fn make_billing(
    bytes: web::Bytes,
    params: web::Path<(String, String)>,
    producer: web::Data<FutureProducer>,
) -> HttpResponse {
    let key = bytes.hash(&mut DefaultHasher::new());

    let result = producer
        .send(
            FutureRecord::to(BILLINGS_TOPIC)
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
