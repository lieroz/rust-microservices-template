use actix_web::{web, HttpRequest, HttpResponse};
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord};

pub fn get_orders(req: HttpRequest) -> HttpResponse {
    // send to api method
    HttpResponse::Ok().finish()
}

pub fn create_order(bytes: web::Bytes, producer: web::Data<FutureProducer>) -> HttpResponse {
    producer.send(
        FutureRecord::to("orders")
            .payload(&String::from_utf8(bytes.to_vec()).unwrap())
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
