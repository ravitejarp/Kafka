package com.ravi.kafka.tutorial1.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignAndSeek {
    public static void main(String[] args) {
        Logger log = LoggerFactory.getLogger(ConsumerDemo.class);

        Properties properties = new Properties();
        String bootstrapServers = "127.0.0.1:9092";
        String firstTopic = "first_topic";
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //Earliest from beginning -- Latest iss only latest
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //Kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //Assign and Seek
        //assign
        TopicPartition topicPartition = new TopicPartition(firstTopic, 0);
        consumer.assign(Arrays.asList(topicPartition));

        //seek
        long offsetToRea = 15L;
        consumer.seek(topicPartition, offsetToRea);
        int numOfMsg = 5;
        boolean read = true;
        int numOfRecords = 0;

        //Poll new data
        while (read) {
            //poll(millis) is depricated
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                numOfRecords++;
                log.info("Key:" + consumerRecord.key() + ",Value:" + consumerRecord.value());
                log.info("Partition:" + consumerRecord.partition() + ",Offset:" + consumerRecord.offset());
                if (numOfMsg == numOfRecords) {
                    read = false;
                    break;
                }
            }
        }

    }
}
