use crate::KafkaTopics;
use actix_web::{web, HttpResponse};
use crypto::digest::Digest;
use crypto::sha2::Sha256;
use futures::*;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord};

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
