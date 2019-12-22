use crate::KafkaTopics;
use futures::stream::Stream;
use rdkafka::client::ClientContext;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::{BorrowedHeaders, Headers, Message, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::collections::HashMap;
use std::sync::Arc;

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

pub fn consume_and_process(
    topics: KafkaTopics,
    producer: FutureProducer,
    consumer: Arc<StreamConsumer<BillingContext>>,
) {
    consumer
        .subscribe(&[&topics.billing_service_topic])
        .expect("Can't subscribe to specified topics");

    for message in consumer.start().wait() {
        match message {
            Err(e) => error!("{}:Error: can't read from kafka stream: {:?}", line!(), e),
            Ok(Err(e)) => error!("{}:Error: kafka error: {}", line!(), e),
            Ok(Ok(msg)) => {
                match msg.payload_view::<str>() {
                    None => {
                        error!("{}:Error: empty payload came from kafka", line!());
                    }
                    Some(Ok(payload)) => {
                        debug!(
                            "payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                            payload,
                            msg.topic(),
                            msg.partition(),
                            msg.offset(),
                            msg.timestamp()
                        );

                        match get_kafka_message_metadata(&msg.headers()) {
                            Ok(metadata) => {
                                let payload = "".to_string();
                                let record: FutureRecord<String, String> =
                                    FutureRecord::to(&topics.orders_service_topic)
                                        .headers(
                                            OwnedHeaders::new()
                                                .add("user_id", metadata["user_id"])
                                                .add("order_id", metadata["order_id"])
                                                .add("operation", "make_billing"),
                                        )
                                        .payload(&payload);

                                let _ = producer.send(record, 0);
                            }
                            Err(e) => error!("{}:Error: {}", line!(), e),
                        }
                    }
                    Some(Err(e)) => {
                        error!(
                            "{}:Error: can't deserialize message payload: {:?}",
                            line!(),
                            e
                        );
                    }
                }

                consumer.commit_message(&msg, CommitMode::Async).unwrap();
            }
        };
    }

    info!(
        "thread id {:?}: stopping kafka consumer thread",
        std::thread::current().id(),
    );
}
