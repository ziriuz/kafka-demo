package dev.siriuz.kafkaspringdemo.service;

import dev.siriuz.model.ActionCompleted;
import dev.siriuz.model.ActionRequested;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
@Slf4j
public class ActionRequestV2Service {

    private static long counter = 0;

    @Value("${kafka.topic.action.requested}")
    private String ACTION_REQUESTED_TOPIC;

    @Autowired
    ActionProcessorCustomReplyingTemplate kafkaProducerConsumer;

    public ActionCompleted processActionRequest(String key, ActionRequested request) {

        try {

            var actionCompleted = kafkaProducerConsumer.sendActionRequest(key, request).get(30, TimeUnit.SECONDS);
            String correlationId = actionCompleted.getCorrelationId();

            log.info(">>>>> <{}> Received Action Completed {}: {}", correlationId, actionCompleted.getRequestId(), actionCompleted);
            counter++;
            log.info("*********** total requests processed: {} *************", counter);
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
