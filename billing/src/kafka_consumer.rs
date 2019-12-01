use crate::db::CreateBilling;
use crate::validation_schema::VALIDATION_SCHEMA_CREATE;
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

pub struct BillingContext;

impl ClientContext for BillingContext {}

impl ConsumerContext for BillingContext {
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
    consumer: Arc<StreamConsumer<BillingContext>>,
    pool: r2d2::Pool<RedisConnectionManager>,
) {
    consumer
        .subscribe(&[&topics.billing_service_topic])
        .expect("Can't subscribe to specified topics");

    let mut scope = Scope::new();
    let validator = scope
        .compile_and_return(VALIDATION_SCHEMA_CREATE.clone(), true)
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

                debug!("key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                      std::str::from_utf8(msg.key().unwrap()).unwrap(),
                      payload, msg.topic(), msg.partition(), msg.offset(), msg.timestamp());

                let mut metadata = HashMap::new();

                // TODO: move to separate function, getKafkaMessageMetadata
                if let Some(headers) = msg.headers() {
                    for i in 0..headers.count() {
                        let header = headers.get(i).unwrap();
                        let key = header.0;
                        let value = std::str::from_utf8(header.1).unwrap();
                        metadata.insert(key, value);
                    }
                }

                // TODO: move to separate function, validateJSONAnd...
                match serde_json::from_str(payload) {
                    Ok(value) => {
                        if validator.validate(&value).is_valid() {
                            let billing: CreateBilling =
                                serde_json::value::from_value(value).unwrap();
                            billing.create(
                                metadata["user_id"],
                                metadata["order_id"],
                                &mut pool.get().unwrap(),
                            );
                        } else {
                            error!("Error: invalid JSON schema: {}", value);
                        }
                    }
                    Err(error) => error!("Error: JSON validation: {}", error),
                }

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
