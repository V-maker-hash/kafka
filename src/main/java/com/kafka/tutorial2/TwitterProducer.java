package com.kafka.tutorial2;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterProducer {
    public static final String URL = "127.0.0.1:9092";
    private static final Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
    protected static final Queue<String> LINKED_BLOCKING_QUEUE = new LinkedBlockingQueue<>(1000);

    public static void main(String[] args) throws InterruptedException {
        ProducerUtil.fillQueue(LINKED_BLOCKING_QUEUE);

        KafkaProducer<String, String> producer = createProducer();

        for (String newRecord : LINKED_BLOCKING_QUEUE) {
            Thread.sleep(1000);
            producer.send(new ProducerRecord<>("twitter_topic", newRecord),
                    new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if (e == null) {
                                logger.info("Received new metadata: \n" +
                                                "Topic: {},\n" +
                                                "Partition: {},\n" +
                                                "Offset: {}, \n" +
                                                "Timestamp: {}",
                                        recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                            } else {
                                logger.error("Error while producing", e);
                            }
                        }
                    });
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("stopping app ...");
            logger.info("closing producer ...");
            producer.close();
            logger.info("done!");
        }));
    }

    private static KafkaProducer<String, String> createProducer() {
        //create producer
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, URL);

        //create safe producer
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        return new KafkaProducer<>(properties);
    }
}
