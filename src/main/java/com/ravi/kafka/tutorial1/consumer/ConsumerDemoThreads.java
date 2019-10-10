package com.ravi.kafka.tutorial1.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


public class ConsumerDemoThreads {
    Logger log = LoggerFactory.getLogger(ConsumerDemoThreads.class);

    public static void main(String[] args) {
        new ConsumerDemoThreads().run();

    }

    private ConsumerDemoThreads() {

    }

    public void run() {
        String firstTopic = "first_topic";
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "Consumer-group-Another";
        CountDownLatch latch = new CountDownLatch(1);
        Runnable consumerThread = new ConsumerThread(latch, firstTopic, bootstrapServers, groupId);
        Thread myThrea = new Thread(consumerThread);
        myThrea.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Caught shutdown hookk");
            ((ConsumerThread) consumerThread).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            log.info("App exited");
        }));
        try {
            latch.await();
        } catch (InterruptedException e) {
            log.error("Exxception:" + e);
        } finally {
            log.info("Closed consumer");
        }
    }

    class ConsumerThread implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        Logger log = LoggerFactory.getLogger(ConsumerThread.class);

        ConsumerThread(CountDownLatch latch, String topic, String bootstrapServers, String groupId) {
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            //Earliest from beginning -- Latest iss only latest
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            //Kafka consumer
            consumer = new KafkaConsumer<>(properties);

            //Subscriber to topics
            consumer.subscribe(Arrays.asList(topic));
            this.latch = latch;

        }

        @Override
        public void run() {
            //Poll new data
            try {
                while (true) {
                    //poll(millis) is depricated
                    ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                        log.info("Key:" + consumerRecord.key() + ",Value:" + consumerRecord.value());
                        log.info("Partition:" + consumerRecord.partition() + ",Offset:" + consumerRecord.offset());
                    }
                }
            } catch (WakeupException e) {
                log.info("Shutdown called");
            } finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown() {
            //Interrupts consumer.poll() and thhrows WakeUpException
            consumer.wakeup();
        }
    }
}
