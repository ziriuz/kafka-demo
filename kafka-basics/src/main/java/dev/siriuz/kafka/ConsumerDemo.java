package dev.siriuz.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    static String formatMessage(String msg){
        return "["+ new Date() +"] " + msg;
    }

    public static void main(String[] args) {
        logger.info("Start...");

        String groupId = "monitor-consumer-group";
        String topic = "monitor";

        Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // to overwrite default re-balance strategy
        consumerProps.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());


        try (Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(topic));

            final Thread mainThread = Thread.currentThread();

            Runtime.getRuntime().addShutdownHook(
                    new Thread(() -> {
                        logger.info("Shutdown detected");
                        consumer.wakeup();
                        try {
                            mainThread.join();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    })
            );

            while(true) {

                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records)
                    System.out.println(formatMessage(record.topic() + ": "
                            + record.key() + ": " + record.value() + ": "
                            + record.partition() + ": " + record.offset()));
            }

        } catch (WakeupException e) {
            logger.info("Consumer wakeup exception handled");
        }
        logger.info("Stopped");

    }

}
