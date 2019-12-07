use crate::db::CreateOrder;
use crate::validation_schema::{VALIDATION_SCHEMA_CREATE, VALIDATION_SCHEMA_UPDATE};
use crate::KafkaTopics;
use futures::stream::Stream;
use r2d2_redis::{r2d2, RedisConnectionManager};
use rdkafka::client::ClientContext;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::{BorrowedHeaders, Headers, Message};
use std::collections::HashMap;
use std::sync::Arc;
use valico::json_schema::{schema, Scope};

pub struct WarehouseContext;

impl ClientContext for WarehouseContext {}

impl ConsumerContext for WarehouseContext {
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
) -> Result<(), Box<dyn std::error::Error>> {
    match validators.get(op) {
        None => Ok(()),
        Some(validator) => match serde_json::from_str(payload) {
            Ok(value) => {
                if validator.validate(&value).is_valid() {
                    if op == "create" {
                        let order: CreateOrder = serde_json::value::from_value(value).unwrap();
                        order.create(metadata["user_id"], &mut pool.get().unwrap())
                    } else if op == "update" {
                        Ok(())
                    } else {
                        error!("{}:Unknown operation: {}", line!(), op);
                        Ok(())
                    }
                } else {
                    error!("{}:Invalid JSON schema: {}", line!(), value);
                    Ok(())
                }
            }
            Err(e) => Err(Box::new(e)),
        },
    }
}

pub fn consume_and_process(
    topics: KafkaTopics,
    consumer: Arc<StreamConsumer<WarehouseContext>>,
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
                                        Ok(_) => (),
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
