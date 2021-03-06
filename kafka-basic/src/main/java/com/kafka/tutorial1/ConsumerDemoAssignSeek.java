package com.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());
        String bootstrapServers = "127.0.0.1:9092";
        String topic = "first_topic";

        //create Consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //assign
        TopicPartition partitionToRead = new TopicPartition(topic, 0);
        long offsetToReadFrom = 15;
        consumer.assign(Collections.singletonList(partitionToRead));

        //seek
        consumer.seek(partitionToRead, offsetToReadFrom);

        int numberOfMsgToRead = 5;
        boolean keepOnReading = true;
        int numberOfMsgToReadSoFar = 0;

        //poll for new data
        while (keepOnReading) {
            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> consumerRecord : poll) {
                logger.info("Key :{},\n" +
                                "Value :{},\n" +
                                "Offset :{},\n" +
                                "Partition :{}\n",
                        consumerRecord.key(), consumerRecord.value(), consumerRecord.offset(), consumerRecord.partition());

                if (++numberOfMsgToReadSoFar >= numberOfMsgToRead) {
                    keepOnReading = false;
                    break;
                }
            }
        }

    }
}
