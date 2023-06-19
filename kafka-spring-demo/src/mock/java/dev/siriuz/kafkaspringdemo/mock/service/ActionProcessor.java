package dev.siriuz.kafkaspringdemo.mock.service;

import dev.siriuz.kafkaspringdemo.mock.config.KafkaSpringConfig;
import dev.siriuz.model.ActionCompleted;
import dev.siriuz.model.ActionRequested;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
public class ActionProcessor {

    @Value("${kafka.topic.action.completed}")
    private String replyTopic;

    @Autowired
    KafkaTemplate<String, ActionCompleted> kafkaTemplate;

    @KafkaListener(groupId="${kafka.consumer.group-id}", topics = "${kafka.topic.demo.action.requested}",
                   containerFactory = KafkaSpringConfig.REPLY_LISTENER_CONTAINER_FACTORY)
    @SendTo("${kafka.topic.demo.action.completed}")
    public Message<ActionCompleted> listen(ConsumerRecord<String, ActionRequested> consumerRecord) {

        ActionCompleted response = ActionCompleted.newBuilder()
                .setCorrelationId(consumerRecord.value().getCorrelationId())
                .setRequestId(consumerRecord.value().getRequestId())
                .setStatus("DEMO_SUCCESS")
                .build();

        return MessageBuilder.withPayload( response )
                .build();
    }

    @KafkaListener(groupId="${kafka.consumer.id}", topics = "${kafka.topic.action.requested}",
                   containerFactory = KafkaSpringConfig.DEFAULT_LISTENER_CONTAINER_FACTORY)
    public void process(ConsumerRecord<String, ActionRequested> consumerRecord) {

        String correlationId = consumerRecord.value().getCorrelationId();
        System.out.println("[INFO] <" + correlationId + "> Action request consumed: " + consumerRecord.value());

        ActionCompleted response = ActionCompleted.newBuilder()
                .setCorrelationId(correlationId)
                .setRequestId(consumerRecord.value().getRequestId())
                .setStatus("DEMO_SUCCESS")
                .build();

        ProducerRecord<String, ActionCompleted> record = new ProducerRecord<>(replyTopic, consumerRecord.key(), response);
        record.headers().add("SESSION_ID", consumerRecord.headers().lastHeader("SESSION_ID").value());

        System.out.println("[INFO] <" + correlationId + "> Sending response: " + response);
        try {
            kafkaTemplate.send(record).get(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            System.out.println("[ERROR] <" + correlationId + "> process interrupted");
            e.printStackTrace();
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            System.out.println("[ERROR] <" + correlationId + "> process failed: " + e.getMessage());
            e.printStackTrace();
        } catch (TimeoutException e) {
            System.out.println("[ERROR] <" + correlationId + "> process failed to send response within 1 sec");
            e.printStackTrace();
        }
    }
}
