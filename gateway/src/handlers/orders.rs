use crate::KafkaTopics;
use actix_web::{web, HttpResponse};
use crypto::digest::Digest;
use crypto::sha2::Sha256;
use futures::*;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord};

pub fn get_orders() -> HttpResponse {
    // send to orders service api method
    HttpResponse::Ok().finish()
}

pub fn create_order(
    bytes: web::Bytes,
    user_id: web::Path<(String)>,
    producer: web::Data<FutureProducer>,
    kafka_topics: web::Data<KafkaTopics>,
) -> HttpResponse {
    let mut hasher = Sha256::new();
    hasher.input(user_id.as_bytes());
    hasher.input(bytes.as_ref());
    let key = hasher.result_str();

    let result = producer
        .send(
            FutureRecord::to(&kafka_topics.orders_service_topic)
                .key(&key)
                .payload(std::str::from_utf8(bytes.as_ref()).unwrap())
                .headers(OwnedHeaders::new().add("user_id", user_id.as_ref())),
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
                "Error occured while sending message to kafka: error: {}, message: {:?}",
                error, message
            );
            HttpResponse::BadRequest().finish()
        }
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
    kafka_topics: web::Data<KafkaTopics>,
) -> HttpResponse {
    let mut hasher = Sha256::new();
    hasher.input(params.0.as_bytes());
    hasher.input(params.1.as_bytes());
    hasher.input(bytes.as_ref());
    let key = hasher.result_str();

    let result = producer
        .send(
            FutureRecord::to(&kafka_topics.orders_service_topic)
                .key(&key[..])
                .payload(std::str::from_utf8(bytes.as_ref()).unwrap())
                .headers(
                    OwnedHeaders::new()
                        .add("user_id", &params.0)
                        .add("id", &params.1),
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
                "Error occured while sending message to kafka: error: {}, message: {:?}",
                error, message
            );
            HttpResponse::BadRequest().finish()
        }
        Err(_) => HttpResponse::InternalServerError().finish(),
    }
}

pub fn add_good_to_order(
    bytes: web::Bytes,
    params: web::Path<(String, String, String)>,
    producer: web::Data<FutureProducer>,
    kafka_topics: web::Data<KafkaTopics>,
) -> HttpResponse {
    let mut hasher = Sha256::new();
    hasher.input(params.0.as_bytes());
    hasher.input(params.1.as_bytes());
    hasher.input(params.2.as_bytes());
    hasher.input(bytes.as_ref());
    let key = hasher.result_str();

    let result = producer
        .send(
            FutureRecord::to(&kafka_topics.orders_service_topic)
                .key(&key[..])
                .payload(std::str::from_utf8(bytes.as_ref()).unwrap())
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
        Ok(Ok(delivery)) => {
            info!(
                "Message sent to kafka: partition: {}, offset: {}",
                delivery.0, delivery.1
            );
            HttpResponse::Created().finish()
        }
        Ok(Err((error, message))) => {
            error!(
                "Error occured while sending message to kafka: error: {}, message: {:?}",
                error, message
            );
            HttpResponse::BadRequest().finish()
        }
        Err(_) => HttpResponse::InternalServerError().finish(),
    }
}

pub fn delete_good_from_order(
    bytes: web::Bytes,
    params: web::Path<(String, String, String)>,
    producer: web::Data<FutureProducer>,
    kafka_topics: web::Data<KafkaTopics>,
) -> HttpResponse {
    let mut hasher = Sha256::new();
    hasher.input(params.0.as_bytes());
    hasher.input(params.1.as_bytes());
    hasher.input(params.2.as_bytes());
    hasher.input(bytes.as_ref());
    let key = hasher.result_str();

    let result = producer
        .send(
            FutureRecord::to(&kafka_topics.orders_service_topic)
                .key(&key[..])
                .payload(std::str::from_utf8(bytes.as_ref()).unwrap())
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
        Ok(Ok(delivery)) => {
            info!(
                "Message sent to kafka: partition: {}, offset: {}",
                delivery.0, delivery.1
            );
            HttpResponse::Created().finish()
        }
        Ok(Err((error, message))) => {
            error!(
                "Error occured while sending message to kafka: error: {}, message: {:?}",
                error, message
            );
            HttpResponse::BadRequest().finish()
        }
        Err(_) => HttpResponse::InternalServerError().finish(),
    }
}
