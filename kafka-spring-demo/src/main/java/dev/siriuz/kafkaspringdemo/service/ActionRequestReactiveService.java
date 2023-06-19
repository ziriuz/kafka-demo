package dev.siriuz.kafkaspringdemo.service;

import dev.siriuz.kafkaspringdemo.config.KafkaSpringConfig;
import dev.siriuz.model.ActionCompleted;
import dev.siriuz.model.ActionRequested;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
@Slf4j
public class ActionRequestReactiveService {

    @Autowired
    Flux<ActionCompleted> streamActionCompleted;

    @Autowired
    @Qualifier(KafkaSpringConfig.DEFAULT_PRODUCER)
    KafkaTemplate<String, SpecificRecord> template;

    @Value("${kafka.topic.action.requested}")
    private String ACTION_REQUESTED_TOPIC;

    public ActionCompleted processActionRequest(String key, ActionRequested request) {

        String correlationId = UUID.randomUUID().toString();
        ProducerRecord<String, SpecificRecord> record = new ProducerRecord<>(ACTION_REQUESTED_TOPIC, key ,request);
        request.setCorrelationId(correlationId);
        record.headers().add("SESSION_ID", correlationId.getBytes());
        SendResult<String, SpecificRecord> sendResult;

        log.info("<{}> Sending Action Request {}: {}", correlationId, key, request);
        try {
            sendResult = template.send(record).get(1, TimeUnit.SECONDS);
            sendResult.getProducerRecord().headers().forEach(
                    header -> log.info(">>>>>>>> <{}> Request header {}: {}", correlationId, header.key(), new String(header.value()))
            );

            var actionCompletedSubscriber = new ActionCompletedSubscriber(correlationId, 3000);
            streamActionCompleted
                    .filter(actionCompleted -> actionCompleted.getCorrelationId().equals(correlationId))
                    .flatMap(Mono::just)
                    .subscribe(actionCompletedSubscriber);

            var actionCompleted = actionCompletedSubscriber.getResult();
            log.info(">>>>> <{}> Received Action Completed {}: {}", correlationId, actionCompleted.getRequestId(), actionCompleted);
            //System.out.printf(">>>>>>>> kafka_correlationId: %s%n",UUID.nameUUIDFromBytes(consumerRecord.headers().lastHeader("kafka_correlationId").value()));
            return actionCompleted;

        } catch (InterruptedException e) {
            log.warn("sending action request was interrupted: {}", request);
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException | TimeoutException e) {
            log.error("sending action request failed: {} {}", request, e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
