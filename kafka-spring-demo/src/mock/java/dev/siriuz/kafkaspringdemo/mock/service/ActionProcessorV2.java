package dev.siriuz.kafkaspringdemo.mock.service;

import dev.siriuz.kafkaspringdemo.mock.config.KafkaSpringConfig;
import dev.siriuz.model.ActionCompleted;
import dev.siriuz.model.ActionRequested;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ActionProcessorV2
{
    @Value("${kafka.topic.action.completed}")
    private String replyTopic;

    @Autowired
    KafkaTemplate<String, ActionCompleted> kafkaTemplate;

    @KafkaListener(groupId="${kafka.consumer.id}", topics = "${kafka.topic.action.requested}",
            containerFactory = KafkaSpringConfig.DEFAULT_LISTENER_CONTAINER_FACTORY)
    public void process(ConsumerRecord<String, ActionRequested> consumerRecord) {

        String correlationId = consumerRecord.value().getCorrelationId();
        log.info("<{}> Action request consumed: {}",correlationId, consumerRecord.value());

        ActionCompleted response = ActionCompleted.newBuilder()
                .setCorrelationId(correlationId)
                .setRequestId(consumerRecord.value().getRequestId())
                .setStatus("DEMO_SUCCESS")
                .build();

        ProducerRecord<String, ActionCompleted> record = new ProducerRecord<>(replyTopic, consumerRecord.key(), response);
        record.headers().add("SESSION_ID", consumerRecord.headers().lastHeader("SESSION_ID").value());

        log.info("<{}> Sending response: {}", correlationId, response);
        kafkaTemplate.send(record);

//        Using asynchronous send not to impact performance
//        ToDo: handle send errors using callback method
//        try {
//            kafkaTemplate.send(record).get(1, TimeUnit.SECONDS);
//        } catch (InterruptedException e) {
//            log.error("<{}> process interrupted", correlationId);
//            e.printStackTrace();
//            Thread.currentThread().interrupt();
//        } catch (ExecutionException e) {
//            log.error("<{}> process failed: {}", correlationId, e.getMessage());
//            e.printStackTrace();
//        } catch (TimeoutException e) {
//            log.error("<{}> process failed to send response within 1 sec", correlationId);
//            e.printStackTrace();
//        }
    }
}
