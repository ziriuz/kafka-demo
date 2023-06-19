package dev.siriuz.kafkaspringdemo.config;

import dev.siriuz.kafkaspringdemo.service.ActionCompletedListener;
import dev.siriuz.model.ActionCompleted;
import dev.siriuz.model.ActionRequested;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.specific.SpecificRecord;
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
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaSpringConfig {

    public static final String SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url";
    public static final String SPECIFIC_AVRO_READER_CONFIG = "specific.avro.reader";

    @Value("${kafka.consumer.group-id}")
    private String CONSUMER_GROUP_ID;

    @Value("${kafka.topic.action.requested}")
    private String ACTION_REQUESTED_TOPIC;

    @Value("${kafka.topic.demo.action.completed}")
    private String ACTION_COMPLETED_TOPIC;

    public static final String REPLYING_PRODUCER = "REPLYING_PRODUCER";
    @Bean(name = REPLYING_PRODUCER)
    public ReplyingKafkaTemplate<String, ActionRequested, ActionCompleted> replyingTemplate(
            ProducerFactory<String, ActionRequested> producerFactory,
            ConcurrentMessageListenerContainer<String, ActionCompleted> replyingContainer) {
        return new ReplyingKafkaTemplate<>(producerFactory, replyingContainer);
    }

    @Bean
    public ConcurrentMessageListenerContainer<String, ActionCompleted> replyingContainer(
            ConcurrentKafkaListenerContainerFactory<String, ActionCompleted> kafkaListenerContainerFactory) {
        ConcurrentMessageListenerContainer<String, ActionCompleted>
                repliesContainer = kafkaListenerContainerFactory.createContainer(ACTION_COMPLETED_TOPIC);
        repliesContainer.getContainerProperties().setGroupId(CONSUMER_GROUP_ID);
        repliesContainer.setAutoStartup(false);
        return repliesContainer;
    }

    public static final String REPLYING_LISTENER_CONTAINER_FACTORY = "REPLYING_LISTENER_CONTAINER_FACTORY";
    @Bean(name = LISTENER_CONTAINER_FACTORY)
    ConcurrentKafkaListenerContainerFactory<String, ActionCompleted>
    kafkaReplyingListenerContainerFactory(ConsumerFactory<String, ActionCompleted> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, ActionCompleted> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        return factory;
    }

    @Bean
    ProducerFactory<String, ActionRequested> producerFactory(){
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

    public static final String DEFAULT_PRODUCER = "DEFAULT_PRODUCER";
    @Bean(DEFAULT_PRODUCER)
    public KafkaTemplate<String, SpecificRecord> kafkaTemplate() {
        return new KafkaTemplate<String, SpecificRecord>(new DefaultKafkaProducerFactory<>(producerProps()));
    }

    public static final String LISTENER_CONTAINER_FACTORY = "LISTENER_CONTAINER_FACTORY";
    @Bean(name = LISTENER_CONTAINER_FACTORY)
    ConcurrentKafkaListenerContainerFactory<String, SpecificRecord> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, SpecificRecord> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(consumerProps()));
        return factory;
    }


    @Bean
    public ActionCompletedListener listener() {
        return new ActionCompletedListener();
    }

}
