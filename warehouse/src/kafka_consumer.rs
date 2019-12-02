use crate::db::CreateOrder;
use crate::validation_schema::{VALIDATION_SCHEMA_CREATE, VALIDATION_SCHEMA_UPDATE};
use crate::KafkaTopics;
use futures::stream::Stream;
use r2d2_redis::{r2d2, RedisConnectionManager};
use rdkafka::client::ClientContext;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::{Headers, Message};
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

// FIXME: remove all unwrap calls and decompose
// make interfaces for testing
pub fn consume_and_process(
    topics: KafkaTopics,
    consumer: Arc<StreamConsumer<WarehouseContext>>,
    pool: r2d2::Pool<RedisConnectionManager>,
) {
    consumer
        .subscribe(&[&topics.orders_service_topic])
        .expect("Can't subscribe to specified topics");

    let mut create_scope = Scope::new();
    let create_validator = create_scope
        .compile_and_return(VALIDATION_SCHEMA_CREATE.clone(), true)
        .ok()
        .unwrap();

    let mut update_scope = Scope::new();
    let update_validator = update_scope
        .compile_and_return(VALIDATION_SCHEMA_UPDATE.clone(), true)
        .ok()
        .unwrap();

    for message in consumer.start().wait() {
        match message {
            Err(_) => error!("Can't read from kafka stream."),
            Ok(Err(e)) => error!("Error: kafka error: {}", e),
            Ok(Ok(msg)) => {
                let payload = match msg.payload_view::<str>() {
                    None => {
                        warn!("Empty payload came from kafka");
                        continue;
                    }
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        error!("Can't deserialize message payload: {:?}", e);
                        continue;
                    }
                };

                debug!("key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                      std::str::from_utf8(msg.key().unwrap()).unwrap(),
                      payload, msg.topic(), msg.partition(), msg.offset(), msg.timestamp());

                let mut operation: Option<&str> = None;
                let mut validator: Option<&schema::ScopedSchema> = None;
                let mut metadata = HashMap::new();

                // TODO: move to separate function, getKafkaMessageMetadata
                if let Some(headers) = msg.headers() {
                    for i in 0..headers.count() {
                        let header = headers.get(i).unwrap();
                        let key = header.0;
                        let value = std::str::from_utf8(header.1).unwrap();

                        if key == "operation" {
                            match value {
                                "create" => validator = Some(&create_validator),
                                "update" => validator = Some(&update_validator),
                                "delete" => validator = None,
                                _ => {
                                    error!("Unknown operation: {}", value);
                                    continue;
                                }
                            }
                            operation = Some(value);
                        } else {
                            metadata.insert(key, value);
                        }
                    }
                }

                if operation == None {
                    warn!("Operation header wasn't passed in kafka metadata");
                    continue;
                }

                let operation = operation.unwrap();

                // TODO: move to separate function, validateJSONAnd...
                match validator {
                    Some(validator) => {
                        match serde_json::from_str(payload) {
                            Ok(value) => {
                                if validator.validate(&value).is_valid() {
                                    if operation == "create" {
                                        let order: CreateOrder =
                                            serde_json::value::from_value(value).unwrap();
                                        order.create(metadata["user_id"], &mut pool.get().unwrap());
                                    } else if operation == "update" {
                                    }
                                } else {
                                    error!("Invalid JSON schema: {}", value);
                                }
                            }
                            Err(error) => error!("JSON validation: {}", error),
                        };
                    }
                    None => if operation == "delete" {},
                };

                // TODO: consumer commits only here, consider deleting all unwrap calls
                // or making commits everywhere
                consumer.commit_message(&msg, CommitMode::Async).unwrap();
            }
        };
    }

    info!(
        "thread id {:?}: stopping kafka consumer thread",
        std::thread::current().id(),
    );
}
