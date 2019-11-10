use actix_web::{web, HttpRequest, HttpResponse};

use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord};

use serde::Deserialize;

#[derive(Deserialize)]
struct Item {
    id: u64,
    amount: u64,
}

#[derive(Deserialize)]
struct Order {
    id: u64,
    goods: Vec<Item>,
}

pub fn get_orders(req: HttpRequest) -> HttpResponse {
    // send to api method
    HttpResponse::Ok().finish()
}

pub fn create_order(req: HttpRequest, producer: web::Data<FutureProducer>) -> HttpResponse {
    // add mpsc send to kafka thread
    producer.send(
        FutureRecord::to("test")
            .payload("payload")
            .key("key")
            .headers(OwnedHeaders::new().add("header_key", "header_value")),
        0,
    );
    HttpResponse::Ok().finish()
}

pub fn get_order_detailed(req: HttpRequest) -> HttpResponse {
    // send to api method
    HttpResponse::Ok().finish()
}

pub fn update_order(req: HttpRequest, producer: web::Data<FutureProducer>) -> HttpResponse {
    // add mpsc send to kafka thread
    HttpResponse::Ok().finish()
}

pub fn add_good_to_order(req: HttpRequest, producer: web::Data<FutureProducer>) -> HttpResponse {
    // add mpsc send to kafka thread
    HttpResponse::Ok().finish()
}

pub fn delete_good_from_order(
    req: HttpRequest,
    producer: web::Data<FutureProducer>,
) -> HttpResponse {
    // add mpsc send to kafka thread
    HttpResponse::Ok().finish()
}
