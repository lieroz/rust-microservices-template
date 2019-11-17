use crate::KafkaTopics;
use futures::stream::Stream;
use rdkafka::client::ClientContext;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::{Headers, Message};
use std::sync::Arc;

pub struct OrdersContext;

impl ClientContext for OrdersContext {}

impl ConsumerContext for OrdersContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        info!(
            "thread id {:?}: Pre rebalance {:?}",
            std::thread::current().id(),
            rebalance
        );
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        info!(
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
        info!(
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

    for message in consumer.start().wait() {
        match message {
            Err(_) => error!("Error while reading from stream."),
            Ok(Err(e)) => error!("Kafka error: {}", e),
            Ok(Ok(m)) => {
                let payload = match m.payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        error!("Error while deserializing message payload: {:?}", e);
                        ""
                    }
                };

                info!("thread id: {:?}", std::thread::current().id());
                info!("key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                      m.key(), payload, m.topic(), m.partition(), m.offset(), m.timestamp());

                if let Some(headers) = m.headers() {
                    for i in 0..headers.count() {
                        let header = headers.get(i).unwrap();
                        let user_id = std::str::from_utf8(header.1).unwrap();

                        info!("  Header {:#?}: {:?}", header.0, user_id);
                    }
                }

                consumer.commit_message(&m, CommitMode::Async).unwrap();
            }
        };
    }

    info!(
        "thread id {:?}: stopping kafka consumer thread",
        std::thread::current().id(),
    );
}
