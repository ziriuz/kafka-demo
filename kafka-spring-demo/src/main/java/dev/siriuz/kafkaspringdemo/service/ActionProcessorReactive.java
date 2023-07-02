package dev.siriuz.kafkaspringdemo.service;

import dev.siriuz.kafkaspringdemo.config.KafkaSpringConfig;
import dev.siriuz.kafkaspringdemo.domain.model.ActionResult;
import dev.siriuz.kafkaspringdemo.domain.model.DomainActionRequest;
import dev.siriuz.kafkaspringdemo.domain.model.LifecycleResult;
import dev.siriuz.kafkaspringdemo.domain.port.RequestResponseProcessor;
import dev.siriuz.kafkaspringdemo.util.DtoUtils;
import dev.siriuz.model.ActionCompleted;
import dev.siriuz.model.ActionRequested;
import dev.siriuz.model.LifecycleCompleted;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

@Service
@Slf4j
public class ActionProcessorReactive implements RequestResponseProcessor<DomainActionRequest, ActionResult> {

    private static long counter = 0;


    private static final
    Sinks.Many<CorrelatedMessage<String, LifecycleCompleted>> lifecycleCompletedEventSink =
        Sinks.many().replay().limit(1);

    private static final
    Flux<CorrelatedMessage<String, LifecycleCompleted>> lifecycleCompletedEventStream =
            lifecycleCompletedEventSink.asFlux()
                .doOnNext( event ->
                        log.debug("Emitted event with correlationId: {}", event.getCorrelationId())
                )
                .doOnComplete(() -> log.debug("ActionCompleted Event Stream is completed"));

    @Autowired
    @Qualifier(KafkaSpringConfig.DEFAULT_PRODUCER)
    KafkaTemplate<String, SpecificRecord> template;

    @Autowired
    Sinks.Many<CorrelatedMessage<String, ActionCompleted>> actionCompletedEventSink;

    @Autowired
    Flux<CorrelatedMessage<String, ActionCompleted>> actionCompletedEventStream;

    @Value("${kafka.topic.action.requested}")
    private String ACTION_REQUESTED_TOPIC;

    @Override
    public ActionResult process(DomainActionRequest request) {
        return DtoUtils.toEntity(
                processActionRequest(DtoUtils.toAvro(request))
        );
    }

    @Override
    public CompletableFuture<ActionResult> processAsync(DomainActionRequest request) {
        return processActionRequestAsync(DtoUtils.toAvro(request)).getFuture();
    }

    private ActionCompleted processActionRequest(ActionRequested request) {

        try {
            var subscriber = processActionRequestAsync(request);
            var actionCompleted = subscriber.getResult();
            log.info("<<<<< <{}> Received Action Completed {}: {}", subscriber.getCorrelationId(), actionCompleted.getRequestId(), actionCompleted);
            counter++;
            log.info("*********** total requests processed: {} *************", counter);
            return actionCompleted;
        }
        catch (TimeoutException e) {
            log.error("sending action request failed: {} {}", request, e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private ReplyingSubscriber<String, ActionCompleted, ActionResult> processActionRequestAsync(ActionRequested request) {

        String correlationId = UUID.randomUUID().toString();
        String key = request.getRequestId();
        ProducerRecord<String, SpecificRecord> record = new ProducerRecord<>(ACTION_REQUESTED_TOPIC, key ,request);
        request.setCorrelationId(correlationId);
        record.headers().add("SESSION_ID", correlationId.getBytes());

        var actionCompletedSubscriber = subscribeToActionCompletedStream(correlationId, 3000);
        var lifecycleCompletedSubscriber = subscribeToLifecycleCompletedStream(correlationId, 3000);

        log.info(">>>>> <{}> Sending Action Request {}: {}", correlationId, key, request);
        template.send(record); //TODO: implement send with callback to handle send result
        return actionCompletedSubscriber;
    }

    @KafkaListener(id = "reactive-service-action-listener", topics = "${kafka.topic.action.completed}", groupId = "ActionRequestReactiveService",
            containerFactory = KafkaSpringConfig.DEFAULT_LISTENER_CONTAINER_FACTORY)
    public void onActionCompleted(ConsumerRecord<String, ActionCompleted> record) {
        log.info("{} consumed message -> {} -> {}","ActionRequestReactiveService", record.key(), record.value());

        actionCompletedEventSink.tryEmitNext(
                actionCompletedToCorrelatedMessage(record)
        );
    }


    @KafkaListener(id = "reactive-service-lifecycle-listener", topics = "${kafka.topic.lifecycle.completed}", groupId = "ActionRequestReactiveService",
            containerFactory = KafkaSpringConfig.DEFAULT_LISTENER_CONTAINER_FACTORY)
    public void onLifecycleCompleted(ConsumerRecord<String, LifecycleCompleted> record) {
        log.info("{} consumed message -> {} -> {}","ActionRequestReactiveService", record.key(), record.value());

        lifecycleCompletedEventSink.tryEmitNext(
                lifecycleCompletedToCorrelatedMessage(record)
        );
    }


    private  ReplyingSubscriber<String, ActionCompleted, ActionResult> subscribeToActionCompletedStream(String correlationId, long timeoutMillis) {
        var subscriber = new ReplyingSubscriber<String, ActionCompleted, ActionResult>(
                correlationId,
                DtoUtils::toEntity,
                Duration.ofMillis(timeoutMillis));
        actionCompletedEventStream
                .filter(actionCompleted -> actionCompleted.getCorrelationId().equals(correlationId))
                .flatMap(Mono::just)
                .subscribe(subscriber);
        return subscriber;
    }

    private  ReplyingSubscriber<String, LifecycleCompleted, LifecycleResult> subscribeToLifecycleCompletedStream(String correlationId, long timeoutMillis) {
        var subscriber = new ReplyingSubscriber<String, LifecycleCompleted, LifecycleResult>(
                correlationId,
                DtoUtils::toEntity,
                Duration.ofMillis(timeoutMillis));
        lifecycleCompletedEventStream
                .filter(lifecycleCompleted -> lifecycleCompleted.getCorrelationId().equals(correlationId))
                .flatMap(Mono::just)
                .subscribe(subscriber);
        return subscriber;
    }


    private CorrelatedMessage<String, ActionCompleted> actionCompletedToCorrelatedMessage(
            ConsumerRecord<String, ActionCompleted> record)
    {
        return new CorrelatedMessage<>() {
            @Override
            public String getCorrelationId() {
                // Correlation strategy is defined here.
                // Header, Key or Field from value can be used
                return record.value().getCorrelationId();
            }
            @Override
            public ActionCompleted getPayload() {
                return record.value();
            }
            public String toString(){
                return record.value().toString();
            }
        };
    }

    private CorrelatedMessage<String, LifecycleCompleted> lifecycleCompletedToCorrelatedMessage(ConsumerRecord<String, LifecycleCompleted> record){
        return new CorrelatedMessage<>() {
            @Override
            public String getCorrelationId() {
                // Correlation strategy is defined here.
                // Header, Key or Field from value can be used
                return new String(record.headers().lastHeader("CORRELATION_ID").value());
            }
            @Override
            public LifecycleCompleted getPayload() {
                return record.value();
            }
            public String toString(){
                return record.value().toString();
            }
        };
    }


}
