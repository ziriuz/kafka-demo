package dev.siriuz.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger logger = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("Start...");

        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        producerProps.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,"3");

        try (Producer<String, String>  producer = new KafkaProducer<>(producerProps)) {

            for (int i = 0; i < 15; i++) {
                String key = "id_" + i;
                producer.send(new ProducerRecord<>("monitor", key, "{\"name\":\"Monitor Sony v" + i + " \", \"price\":\"1100\"}"),
                        new Callback() { // can be replaced by lambda
                            @Override
                            public void onCompletion(RecordMetadata metadata, Exception e) {
                                System.out.println(metadata.topic() + ": " + key + ": " + metadata.partition() + ": " + metadata.offset());
                            }
                        });
            }

            // producer.flush(); // to block and wait when send is completed
            // producer.close(); // try with resources will do it
        }




    }

}
