use crate::KafkaTopics;
use futures::stream::Stream;
use rdkafka::client::ClientContext;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::{Headers, Message};
use serde_json::Value;
use std::sync::Arc;
use valico::json_schema;

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

lazy_static! {
    static ref VALIDATION_SCHEMA: Value = serde_json::from_str(
        r#"
        {
            "type": "object",
            "properties": {
                "order_id": {
                    "type": "number"
                },
                "goods": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "good_id": {
                                "type": "number"
                            },
                            "price": {
                                "type": "number"
                            },
                            "name": {
                                "type": "string"
                            },
                            "description": {
                                "type": "string"
                            }
                        },
                        "required": ["good_id", "price", "name", "description"]
                    }
                }
            },
            "required": ["order_id", "goods"]
        }"#,
    )
    .unwrap();
}

pub fn consume_and_process(topics: KafkaTopics, consumer: Arc<StreamConsumer<OrdersContext>>) {
    consumer
        .subscribe(&[&topics.orders_service_topic])
        .expect("Can't subscribe to specified topics");

    let mut scope = json_schema::Scope::new();
    let validator = scope
        .compile_and_return(VALIDATION_SCHEMA.clone(), true)
        .ok()
        .unwrap();

    for message in consumer.start().wait() {
        match message {
            Err(_) => error!("Error while reading from stream."),
            Ok(Err(e)) => error!("Kafka error: {}", e),
            Ok(Ok(msg)) => {
                let payload = match msg.payload_view::<str>() {
                    None => {
                        warn!("empty payload came from kafka");
                        continue;
                    }
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        error!("Error while deserializing message payload: {:?}", e);
                        continue;
                    }
                };

                info!("key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                      std::str::from_utf8(msg.key().unwrap()).unwrap(), payload, msg.topic(), msg.partition(), msg.offset(), msg.timestamp());

                if let Some(headers) = msg.headers() {
                    for i in 0..headers.count() {
                        let header = headers.get(i).unwrap();
                        let key = header.0;
                        let value = std::str::from_utf8(header.1).unwrap();

                        info!("Header {}: {}", key, value);
                    }
                }

                match serde_json::from_str(payload) {
                    Ok(value) => {
                        info!("{:?}", value);
                        info!("Is valid: {}", validator.validate(&value).is_valid());
                    }
                    Err(error) => error!("JSON validation error: {}", error),
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
