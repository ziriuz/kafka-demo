package dev.siriuz.kafkaspringdemo.service;

import dev.siriuz.model.ActionCompleted;
import dev.siriuz.model.ActionRequested;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
public class ActionRequestService {
    @Autowired
    ReplyingKafkaTemplate<String, ActionRequested, ActionCompleted> template;

    @Value("${kafka.topic.action.requested}")
    private String ACTION_REQUESTED_TOPIC;

    public ActionCompleted sendActionRequest(String key, ActionRequested request) throws Exception {

        System.out.printf(">>>>> Sending Action Request %s: %s%n", key, request);

        ProducerRecord<String, ActionRequested> record = new ProducerRecord<>(ACTION_REQUESTED_TOPIC, key ,request);
        RequestReplyFuture<String, ActionRequested, ActionCompleted> replyFuture = template.sendAndReceive(record);
        SendResult<String, ActionRequested> sendResult = replyFuture.getSendFuture().get(10, TimeUnit.SECONDS);

        sendResult.getProducerRecord().headers().forEach(
                header -> System.out.printf(">>>>>>>> Request header %s: %s%n", header.key(), new String(header.value()))
        );
        ConsumerRecord<String, ActionCompleted> consumerRecord = replyFuture.get(10, TimeUnit.SECONDS);

        System.out.printf(">>>>> Received Action Completed %s: %s%n", consumerRecord.key(), consumerRecord.value());
        consumerRecord.headers().forEach(
                header -> System.out.printf(">>>>>>>> Response header %s: %s%n", header.key(), new String(header.value()))
        );
        return consumerRecord.value();
    }
}
