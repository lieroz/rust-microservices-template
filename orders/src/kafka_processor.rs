use crate::db::{delete_order, CreateOrder, UpdateOrder};
use crate::validation_schema::{VALIDATION_SCHEMA_CREATE, VALIDATION_SCHEMA_UPDATE};
use crate::KafkaTopics;
use futures::stream::Stream;
use r2d2_redis::{r2d2, RedisConnectionManager};
use rdkafka::client::ClientContext;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::{BorrowedHeaders, Headers, Message, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::sync::Arc;
use valico::json_schema::{schema, Scope};

pub struct OrdersContext;

impl ClientContext for OrdersContext {}

impl ConsumerContext for OrdersContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        debug!(
            "thread id {:?}: Pre rebalance {:?}",
            std::thread::current().id(),
            rebalance
        );
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        debug!(
            "thread id {:?}: Post rebalance {:?}",
            std::thread::current().id(),
            rebalance
        );
    }

    fn commit_callback(
        &self,
        result: KafkaResult<()>,
        _offsets: *mut rdkafka_sys::RDKafkaTopicPartitionList,
    ) {
        debug!(
            "thread id {:?}: Committing offsets: {:?}",
            std::thread::current().id(),
            result
        );
    }
}

fn get_kafka_message_metadata<'a>(
    headers: &'a Option<&BorrowedHeaders>,
) -> Result<HashMap<&'a str, &'a str>, Box<dyn std::error::Error>> {
    let mut metadata = HashMap::new();

    if let Some(headers) = headers {
        for i in 0..headers.count() {
            let header = headers.get(i).unwrap();
            let key = header.0;
            let value = std::str::from_utf8(header.1)?;
            metadata.insert(key, value);
        }
    }

    Ok(metadata)
}

fn process_operation(
    validators: &HashMap<&str, schema::ScopedSchema>,
    op: &str,
    metadata: &HashMap<&str, &str>,
    payload: &str,
    pool: &r2d2::Pool<RedisConnectionManager>,
) -> Result<(Option<serde_json::Value>, &'static str), Box<dyn std::error::Error>> {
    match validators.get(op) {
        None => {
            let _ = delete_order(
                metadata["user_id"],
                metadata["order_id"],
                &mut pool.get().unwrap(),
            )?;
            Ok((None, "delete"))
        }
        Some(validator) => match serde_json::from_str(payload) {
            Ok(mut value) => {
                if validator.validate(&value).is_valid() {
                    if op == "create" {
                        let order: CreateOrder =
                            serde_json::value::from_value(value.clone()).unwrap();
                        let order_id =
                            order.create(metadata["user_id"], &mut pool.get().unwrap())?;
                        value.as_object_mut().unwrap().insert(
                            "id".to_string(),
                            serde_json::Value::Number(serde_json::Number::from(order_id)),
                        );
                        Ok((Some(value), "create"))
                    } else if op == "update" {
                        let order: UpdateOrder =
                            serde_json::value::from_value(value.clone()).unwrap();
                        let _ = order.update(
                            metadata["user_id"],
                            metadata["order_id"],
                            &mut pool.get().unwrap(),
                        )?;
                        value.as_object_mut().unwrap().insert(
                            "id".to_string(),
                            serde_json::Value::String(metadata["order_id"].to_string()),
                        );
                        Ok((Some(value), "update"))
                    } else {
                        Err(Box::new(Error::new(
                            ErrorKind::Other,
                            format!("{}:Unknown operation: {}", line!(), op),
                        )))
                    }
                } else {
                    Err(Box::new(Error::new(
                        ErrorKind::Other,
                        format!("{}:Invalid JSON schema: {}", line!(), value),
                    )))
                }
            }
            Err(e) => Err(Box::new(e)),
        },
    }
}

pub fn consume_and_process(
    topics: KafkaTopics,
    producer: FutureProducer,
    consumer: Arc<StreamConsumer<OrdersContext>>,
    pool: r2d2::Pool<RedisConnectionManager>,
) {
    consumer
        .subscribe(&[&topics.orders_service_topic])
        .expect("Can't subscribe to specified topics");
    let mut validators = HashMap::new();

    let mut create_scope = Scope::new();
    let create_validator = create_scope
        .compile_and_return(VALIDATION_SCHEMA_CREATE.clone(), true)
        .unwrap();
    validators.insert("create", create_validator);

    let mut update_scope = Scope::new();
    let update_validator = update_scope
        .compile_and_return(VALIDATION_SCHEMA_UPDATE.clone(), true)
        .unwrap();
    validators.insert("update", update_validator);

    for message in consumer.start().wait() {
        match message {
            Err(e) => error!("{}:Can't read from kafka stream: {:?}", line!(), e),
            Ok(Err(e)) => error!("{}:Error: kafka error: {}", line!(), e),
            Ok(Ok(msg)) => {
                match msg.payload_view::<str>() {
                    None => {
                        error!("{}:Empty payload came from kafka", line!());
                    }
                    Some(Ok(payload)) => {
                        debug!("key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                              std::str::from_utf8(msg.key().unwrap()).unwrap(),
                              payload, msg.topic(), msg.partition(), msg.offset(), msg.timestamp());

                        match get_kafka_message_metadata(&msg.headers()) {
                            Ok(metadata) => match metadata.get("operation") {
                                Some(op) => {
                                    match process_operation(
                                        &validators,
                                        op,
                                        &metadata,
                                        payload,
                                        &pool,
                                    ) {
                                        Ok((value, op)) => {
                                            let mut record: FutureRecord<String, String> =
                                                FutureRecord::to(&topics.warehouse_service_topic)
                                                    .headers(
                                                        OwnedHeaders::new()
                                                            .add("user_id", metadata["user_id"])
                                                            .add("operation", op),
                                                    );

                                            let payload;

                                            if let Some(value) = value {
                                                payload = value.to_string();
                                                record = record.payload(&payload);
                                            }

                                            let _ = producer.send(record, 0);
                                        }
                                        Err(e) => error!("{}:Error: {}", line!(), e),
                                    }
                                }
                                None => {
                                    error!("{}:Operation type wasn't passed in message", line!())
                                }
                            },
                            Err(e) => {
                                error!("{}:Can't parse kafka message headers: {}", line!(), e)
                            }
                        }
                    }
                    Some(Err(e)) => {
                        error!("{}:Can't deserialize message payload: {:?}", line!(), e)
                    }
                }

                consumer.commit_message(&msg, CommitMode::Async).unwrap();
            }
        }
    }

    info!(
        "thread id {:?}: stopping kafka consumer thread",
        std::thread::current().id(),
    );
}
