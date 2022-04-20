package com.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        String bootstrapServers = "127.0.0.1:9092";

        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String, String> stringStringKafkaProducer = new KafkaProducer<>(properties);

        for (int i = 0; i < 100; i++) {
            //create a producer record
            String topic = "first_topic";
            String value = "hello world " + i;
            String key;
            ProducerRecord<String, String> record;
            if (i % 2 == 0) {
                key = "id_" + 2;
            } else {
                key = "id_" + 1;
            }
            record = new ProducerRecord<>(topic, key, value);

            logger.info("Key : {}", key);
            //send data
            stringStringKafkaProducer.send(record, new Callback() {
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
            }).get();
        }
        //flush data
        stringStringKafkaProducer.flush();
        //flush and send data
        stringStringKafkaProducer.close();

    }
}
