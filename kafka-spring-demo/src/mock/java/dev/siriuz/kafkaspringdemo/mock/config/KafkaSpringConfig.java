package dev.siriuz.kafkaspringdemo.mock.config;

import dev.siriuz.kafkaspringdemo.mock.service.ActionCompletedProducerInterceptor;
import dev.siriuz.model.ActionCompleted;
import dev.siriuz.kafkaspringdemo.mock.service.ActionProcessor;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaSpringConfig {
    public static final String SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url";
    public static final String SPECIFIC_AVRO_READER_CONFIG = "specific.avro.reader";

    @Value("${kafka.consumer.group-id}")
    private String CONSUMER_GROUP_ID;


    @Bean
    public KafkaTemplate<String, ActionCompleted> kafkaTemplate(ProducerFactory<String, ActionCompleted> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    public static final String DEFAULT_LISTENER_CONTAINER_FACTORY = "DEFAULT_LISTENER_CONTAINER_FACTORY";
    @Bean(DEFAULT_LISTENER_CONTAINER_FACTORY)
    public ConcurrentKafkaListenerContainerFactory<String, ActionCompleted>
    kafkaListenerContainerFactory(ConsumerFactory<String, ActionCompleted> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, ActionCompleted> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        return factory;
    }

    public static final String REPLY_LISTENER_CONTAINER_FACTORY = "REPLY_LISTENER_CONTAINER_FACTORY";
    @Bean(REPLY_LISTENER_CONTAINER_FACTORY)
    public ConcurrentKafkaListenerContainerFactory<String, ActionCompleted>
    kafkaReplyListenerContainerFactory(ConsumerFactory<String, ActionCompleted> consumerFactory,
                     KafkaTemplate<String, ActionCompleted> kafkaTemplate) {
        ConcurrentKafkaListenerContainerFactory<String, ActionCompleted> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setReplyTemplate(kafkaTemplate);
        return factory;
    }

    @Bean
    public ProducerFactory<String, ActionCompleted> producerFactory(){
        return new DefaultKafkaProducerFactory<>(producerProps());
    }

    @Bean
    public ConsumerFactory<String, ActionCompleted> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerProps());
    }


    private Map<String, Object> producerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(SPECIFIC_AVRO_READER_CONFIG, true);
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                ActionProcessor.class.getName()



        );
        return props;
    }

    private Map<String, Object> consumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(SPECIFIC_AVRO_READER_CONFIG, true);
        return props;
    }

}
