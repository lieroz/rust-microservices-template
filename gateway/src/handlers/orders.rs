use actix_web::{web, HttpRequest, HttpResponse};

use rdkafka::producer::{FutureProducer, FutureRecord};

use rdkafka::message::OwnedHeaders;

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
