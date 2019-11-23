use crate::validation_schema::{VALIDATION_SCHEMA_CREATE, VALIDATION_SCHEMA_UPDATE};
use crate::KafkaTopics;
use futures::stream::Stream;
use rdkafka::client::ClientContext;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::{Headers, Message};
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

pub fn consume_and_process(topics: KafkaTopics, consumer: Arc<StreamConsumer<OrdersContext>>) {
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
            Err(_) => error!("Error: can't read from kafka stream."),
            Ok(Err(e)) => error!("Error: kafka error: {}", e),
            Ok(Ok(msg)) => {
                let payload = match msg.payload_view::<str>() {
                    None => {
                        warn!("Warning: empty payload came from kafka");
                        continue;
                    }
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        error!("Error: can't deserialize message payload: {:?}", e);
                        continue;
                    }
                };

                info!("key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                      std::str::from_utf8(msg.key().unwrap()).unwrap(),
                      payload, msg.topic(), msg.partition(), msg.offset(), msg.timestamp());

                let mut operation: Option<&str> = None;
                let mut validator: Option<&schema::ScopedSchema> = None;

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
                                    error!("Error: unknown operation: {}", value);
                                    continue;
                                }
                            }
                            operation = Some(value);
                        }

                        info!("Header {}: {}", key, value);
                    }
                }

                if operation == None {
                    warn!("Warning: operation header wasn't passed in kafka metadata");
                    continue;
                }

                let operation = operation.unwrap();

                match serde_json::from_str(payload) {
                    Ok(value) => match validator {
                        Some(validator) => {
                            info!("{:?}", value);

                            if validator.validate(&value).is_valid() {
                                if operation == "create" {
                                    info!("send to create method");
                                } else if operation == "update" {
                                    info!("send to update method");
                                }
                            }
                        }
                        None => {
                            if operation == "delete" {
                                info!("send to delete method");
                            }
                        }
                    },
                    Err(error) => error!("Error: JSON validation: {}", error),
                };

                consumer.commit_message(&msg, CommitMode::Async).unwrap();
            }
        };
    }

    info!(
        "thread id {:?}: stopping kafka consumer thread",
        std::thread::current().id(),
    );
}
