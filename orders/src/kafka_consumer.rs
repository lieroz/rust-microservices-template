use crate::{KafkaConsumerOptions, KafkaTopics};
use futures::stream::Stream;
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::{Headers, Message};

struct OrdersContext;

impl ClientContext for OrdersContext {}

impl ConsumerContext for OrdersContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        println!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        println!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(
        &self,
        result: KafkaResult<()>,
        _offsets: *mut rdkafka_sys::RDKafkaTopicPartitionList,
    ) {
        println!("Committing offsets: {:?}", result);
    }
}

pub fn consume_and_process(options: KafkaConsumerOptions, topics: KafkaTopics) {
    let consumer: StreamConsumer<OrdersContext> = ClientConfig::new()
        .set("group.id", &options.group_id)
        .set("bootstrap.servers", &options.bootstrap_servers)
        .set("enable.partition.eof", &options.enable_partition_eof)
        .set("session.timeout.ms", &options.session_timeout_ms)
        .set("enable.auto.commit", &options.enable_auto_commit)
        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(OrdersContext)
        .expect("Consumer creation failed");

    consumer
        .subscribe(&[&topics.orders_service_topic])
        .expect("Can't subscribe to specified topics");

    for message in consumer.start().wait() {
        match message {
            Err(_) => println!("Error while reading from stream."),
            Ok(Err(e)) => println!("Kafka error: {}", e),
            Ok(Ok(m)) => {
                let payload = match m.payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        println!("Error while deserializing message payload: {:?}", e);
                        ""
                    }
                };
                println!("thread id: {:?}", std::thread::current().id());
                println!("key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                      m.key(), payload, m.topic(), m.partition(), m.offset(), m.timestamp());
                if let Some(headers) = m.headers() {
                    for i in 0..headers.count() {
                        let header = headers.get(i).unwrap();
                        let user_id = std::str::from_utf8(header.1).unwrap();
                        println!("  Header {:#?}: {:?}", header.0, user_id);
                    }
                }
                consumer.commit_message(&m, CommitMode::Async).unwrap();
            }
        };
    }
}
