package dev.siriuz.kafkaspringdemo.mock.service;

import dev.siriuz.kafkaspringdemo.mock.config.KafkaSpringConfig;
import dev.siriuz.model.ActionCompleted;
import dev.siriuz.model.ActionRequested;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;

@Component
@Slf4j
public class ActionProcessor implements ProducerInterceptor<String, ActionCompleted> {

    private final ConcurrentMap<String, ConsumerRecord<String, ActionRequested> > requestsCache = new ConcurrentHashMap<>();

    @Value("${kafka.topic.demo.action.completed}")
    private String DEMO_ACTION_COMPLETED_TOPIC;

    @KafkaListener(groupId="${kafka.consumer.group-id}", topics = "${kafka.topic.demo.action.requested}",
                   containerFactory = KafkaSpringConfig.REPLY_LISTENER_CONTAINER_FACTORY)
    @SendTo("${kafka.topic.demo.action.completed}")
    public Message<ActionCompleted> onMessage(ConsumerRecord<String, ActionRequested> consumerRecord) {

        String correlationId = UUID.nameUUIDFromBytes(consumerRecord.headers().lastHeader("kafka_correlationId").value()).toString();
        System.out.println("======ActionProcessor consumer======");
        log.info("topic <{}>, key <{}>, correlationId <{}>: Action request consumed: {}",
                consumerRecord.topic(),consumerRecord.key(), correlationId, consumerRecord.value());
        this.requestsCache.put(correlationId, consumerRecord);

        ActionCompleted response = ActionCompleted.newBuilder()
                .setCorrelationId(consumerRecord.value().getCorrelationId())
                .setRequestId(consumerRecord.value().getRequestId())
                .setStatus("DEMO_SUCCESS")
                .build();

        return MessageBuilder.withPayload( response )
                .build();
    }



    @Override
    public ProducerRecord<String, ActionCompleted> onSend(ProducerRecord<String, ActionCompleted> record) {

        if (!record.topic().equals(DEMO_ACTION_COMPLETED_TOPIC))
            return record;

        String correlationId = UUID.nameUUIDFromBytes(record.headers().lastHeader("kafka_correlationId").value()).toString();
        ConsumerRecord<String, ActionRequested> consumerRecord = this.requestsCache.remove(correlationId);

        if (consumerRecord != null) {
            log.info("topic <{}>, key <{}>, correlationId <{}>: sending Action completed event: {}",
                    record.topic(),consumerRecord.key(), correlationId, record.value());
            return new ProducerRecord<>(record.topic(), consumerRecord.key(), record.value());
        }

        log.info("topic <{}>, key <{}>, correlationId <{}>: sending Action completed event: {}",
                record.topic(),record.key(), correlationId, record.value());
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
