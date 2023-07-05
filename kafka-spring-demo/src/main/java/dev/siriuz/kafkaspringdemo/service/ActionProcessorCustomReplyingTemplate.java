package dev.siriuz.kafkaspringdemo.service;


import dev.siriuz.kafkaspringdemo.config.KafkaSpringConfig;
import dev.siriuz.model.ActionCompleted;
import dev.siriuz.model.ActionRequested;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.requestreply.KafkaReplyTimeoutException;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

@Slf4j
@Service
public class ActionProcessorCustomReplyingTemplate {

    private final String ID = "action-completed-listener";

    private ConcurrentMap<Object, CompletableFuture<ActionCompleted>> futures = new ConcurrentHashMap<>();

    private TaskScheduler scheduler = new ThreadPoolTaskScheduler();

    @Value("${kafka.topic.action.requested}")
    private String ACTION_REQUESTED_TOPIC;

    @Autowired
    @Qualifier(KafkaSpringConfig.DEFAULT_PRODUCER)
    KafkaTemplate<String, SpecificRecord> template;

    @Autowired
    Sinks.Many<ActionCompleted> actionCompletedSink;

    public ActionProcessorCustomReplyingTemplate() {
        ((ThreadPoolTaskScheduler) this.scheduler).initialize();
    }

    @KafkaListener(id = ID, topics = "${kafka.topic.action.completed}", groupId = "${kafka.consumer.id}",
            containerFactory = KafkaSpringConfig.BATCH_LISTENER_CONTAINER_FACTORY)
    public void listenActionCompleted(List<ConsumerRecord<String, ActionCompleted>> records) {

        for (ConsumerRecord<String, ActionCompleted> rec : records ){
             this.reply(rec);
        }

    }
    private void reply(ConsumerRecord<String, ActionCompleted> record) {
        log.info("{} consumed message -> {} -> {}",ID, record.key(), record.value());

        String correlationId = record.value().getCorrelationId();
        if (correlationId == null) {
            log.error("No correlationId found in reply: " + KafkaUtils.format(record)
                    + " - to use request/reply semantics, the responding server must return the correlation id "
                    + " in the correlationId attribute");
        }

        else {
            CompletableFuture<ActionCompleted> future = this.futures.remove(correlationId);
            if (future == null) {
                log.warn("<{}> Late arrival {}", correlationId, record.value());
            } else {
                log.info("Received: {}  WITH_CORRELATION_ID {} ", KafkaUtils.format(record),correlationId);
                future.complete(record.value());
            }
        }
    }

    public Future<ActionCompleted> sendActionRequest(String key, ActionRequested request) {


        String correlationId = UUID.randomUUID().toString();
        ProducerRecord<String, SpecificRecord> record = new ProducerRecord<>(ACTION_REQUESTED_TOPIC, key ,request);
        request.setCorrelationId(correlationId);
        record.headers().add("SESSION_ID", correlationId.getBytes());

        CompletableFuture<ActionCompleted> future = new CompletableFuture<>();

        log.info(">>>>> <{}> Sending Action Request {}: {}", request.getCorrelationId(), key, request);
        this.futures.put(correlationId, future);
        try {
            template.send(record);
        }
        catch (Exception e) {
            this.futures.remove(correlationId);
            throw new KafkaException("Send failed", e);
        }
        scheduleTimeout(record, correlationId, Duration.ofSeconds(30));

        return future;

    }


    private void scheduleTimeout(ProducerRecord<String, SpecificRecord> record, Object correlationId, Duration replyTimeout) {
        this.scheduler.schedule(() -> {
            CompletableFuture<ActionCompleted> removed = this.futures.remove(correlationId);
            if (removed != null) {
                log.warn("Reply timed out for: {} WITH_CORRELATION_ID {}",KafkaUtils.format(record),correlationId);
                removed.completeExceptionally(new KafkaReplyTimeoutException("Reply timed out"));
            }
        }, Instant.now().plus(replyTimeout));
    }
}
