package dev.siriuz.producer.config;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class KafkaProducerConfig {

    private final Properties producerProps = new Properties();
    @Value("${kafka.bootstrap.servers}")
    private String bootstrapServers;

    @Value("${kafka.topic.wikimedia}")
    private String topic;

    @Bean
    public Producer<String, String> wikimediaProducer() {
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // set safe producer config (Kafka <= 2.8)
        producerProps.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerProps.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.setProperty(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
        producerProps.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // to increase throughput
        producerProps.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        producerProps.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        producerProps.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(32 * 1024));

        return new KafkaProducer<>(producerProps);
    }

    @Bean
    public String wikimediaTopic(){
        return topic;
    }

}
