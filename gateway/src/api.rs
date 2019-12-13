use crate::{KafkaTopics, ServicesParams};
use actix_web::{client::Client, web, Error, HttpRequest, HttpResponse};
use crypto::digest::Digest;
use crypto::sha2::Sha256;
use futures::*;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord};

fn client_request(client: &Client, path: &str) -> impl Future<Item = HttpResponse, Error = Error> {
    client
        .get(path)
        .header("User-Agent", "Actix-web-gateway")
        .send()
        .from_err()
        .and_then(|mut response| {
            response
                .body()
                .from_err()
                .and_then(move |body| match std::str::from_utf8(&body) {
                    Ok(s) => {
                        println!("{:?}", response.status());
                        if response.status() != actix_web::http::StatusCode::OK {
                            HttpResponse::NotFound().finish()
                        } else {
                            HttpResponse::Ok()
                                .content_type("application/json")
                                .body(format!("{}", s))
                        }
                    }
                    Err(e) => {
                        error!("{}:Couldn't deserialize payload: {}", line!(), e);
                        HttpResponse::InternalServerError().finish()
                    }
                })
        })
}

pub fn get_orders(
    req: HttpRequest,
    client: web::Data<Client>,
    services_params: web::Data<ServicesParams>,
) -> impl Future<Item = HttpResponse, Error = Error> {
    client_request(
        &client,
        &format!(
            "http://{}{}",
            services_params.orders_service_addr,
            req.path()
        ),
    )
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

pub fn get_order(
    req: HttpRequest,
    client: web::Data<Client>,
    services_params: web::Data<ServicesParams>,
) -> impl Future<Item = HttpResponse, Error = Error> {
    client_request(
        &client,
        &format!(
            "http://{}{}",
            services_params.orders_service_addr,
            req.path()
        ),
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

pub fn get_goods(
    req: HttpRequest,
    client: web::Data<Client>,
    services_params: web::Data<ServicesParams>,
) -> impl Future<Item = HttpResponse, Error = Error> {
    client_request(
        &client,
        &format!(
            "http://{}{}?{}",
            services_params.warehouse_service_addr,
            req.path(),
            req.query_string()
        ),
    )
}

pub fn get_good(
    req: HttpRequest,
    client: web::Data<Client>,
    services_params: web::Data<ServicesParams>,
) -> impl Future<Item = HttpResponse, Error = Error> {
    client_request(
        &client,
        &format!(
            "http://{}{}",
            services_params.warehouse_service_addr,
            req.path()
        ),
    )
}
