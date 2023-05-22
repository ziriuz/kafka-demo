package dev.siriuz.kafkaspringdemo.mock.service;

import dev.siriuz.model.ActionCompleted;
import dev.siriuz.model.ActionRequested;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

@Component
public class ActionProcessor {

    @KafkaListener(groupId="${kafka.consumer.group-id}", topics = "${kafka.topic.action.requested}")
    @SendTo
    public Message<ActionCompleted> listen(ConsumerRecord<String, ActionRequested> consumerRecord) {

        ActionCompleted response = ActionCompleted.newBuilder()
                .setRequestId(consumerRecord.value().getRequestId())
                .setStatus("DEMO_SUCCESS")
                .build();

        return MessageBuilder.withPayload( response )
                .build();
    }

}
