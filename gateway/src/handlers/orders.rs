use actix_web::{web, HttpRequest, HttpResponse};

use futures::*;

use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord};

use tokio::runtime::current_thread;

pub fn get_orders(req: HttpRequest) -> HttpResponse {
    // send to api method
    HttpResponse::Ok().finish()
}

pub fn create_order(bytes: web::Bytes, producer: web::Data<FutureProducer>) -> HttpResponse {
    let producer_future = producer
        .send(
            FutureRecord::to("orders")
                .payload(&String::from_utf8(bytes.to_vec()).unwrap())
                .key("key")
                .headers(OwnedHeaders::new().add("header_key", "header_value")),
            0,
        )
        .then(|result| {
            match result {
                Ok(Ok(delivery)) => println!("Sent: {:?}", delivery),
                Ok(Err((e, _))) => println!("Error: {:?}", e),
                Err(_) => println!("Future cancelled"),
            }
            Ok(())
        });
    let _ = current_thread::Runtime::new()
        .unwrap()
        .handle()
        .spawn(producer_future);
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
