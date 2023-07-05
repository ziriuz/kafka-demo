package dev.siriuz.kafkaspringdemo;

import dev.siriuz.kafkaspringdemo.domain.model.ActionResult;
import dev.siriuz.kafkaspringdemo.service.ActionCompletedSubscriber;
import dev.siriuz.kafkaspringdemo.service.CorrelatedMessage;
import dev.siriuz.kafkaspringdemo.service.ReplyingSubscriber;
import dev.siriuz.kafkaspringdemo.util.DtoUtils;
import dev.siriuz.model.ActionCompleted;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ReactorTest {

    @Test
    public void tryOutReactorTest() throws InterruptedException, ExecutionException, TimeoutException {

        Sinks.Many<ActionCompleted> actionCompletedSink = Sinks.many()
                .multicast()
                .onBackpressureBuffer();

        Flux<ActionCompleted> actionCompletedFlux = actionCompletedSink.asFlux()
                .doOnNext(actionCompleted -> System.out.println(
                       "[DEBUG] (" + Thread.currentThread().getName() + ") Emitted event with requestId:" +
                               actionCompleted.getRequestId()
                ))
                .doOnComplete(
                        () -> System.out.println(
                                "[DEBUG] (" + Thread.currentThread().getName() + ") Flux completed:"
                ))
                ;

        ActionCompletedSubscriber sub05 = new ActionCompletedSubscriber("05");
        ActionCompletedSubscriber sub08 = new ActionCompletedSubscriber("08");
        ActionCompletedSubscriber sub09 = new ActionCompletedSubscriber("09",1000);

        actionCompletedFlux
                .filter(actionCompleted -> actionCompleted.getRequestId().equals("05"))
                .flatMap(Mono::just)
                .subscribe(sub05);

        actionCompletedSink.tryEmitNext(buildActionCompletedEvent("01"));
        actionCompletedSink.tryEmitNext(buildActionCompletedEvent("02"));
        actionCompletedSink.tryEmitNext(buildActionCompletedEvent("03"));

        actionCompletedFlux
                .filter(actionCompleted -> actionCompleted.getRequestId().equals("08"))
                .flatMap(Mono::just)
                .subscribe(sub08);

        actionCompletedFlux
                .filter(actionCompleted -> actionCompleted.getRequestId().equals("09"))
                .flatMap(Mono::just)
                .subscribe(sub09);

        new Thread(() -> {

            try {
                System.out.println("[INFO] completed09: start");
                ActionCompleted completed09 = sub09.getResult();
                System.out.println("[INFO] completed09: " + completed09);
            } catch (TimeoutException e) {
                System.out.println("[ERROR] completed09: timeout waiting for action completion");
            }

        }).start();
        //ActionCompleted completed09 = sub09.getResult();

        actionCompletedSink.tryEmitNext(buildActionCompletedEvent("04"));
        actionCompletedSink.tryEmitNext(buildActionCompletedEvent("05"));
        actionCompletedSink.tryEmitNext(buildActionCompletedEvent("06"));
        actionCompletedSink.tryEmitNext(buildActionCompletedEvent("07"));
        actionCompletedSink.tryEmitNext(buildActionCompletedEvent("08"));

        sub05.request(1);

        Thread.sleep(1100);

        actionCompletedSink.tryEmitNext(buildActionCompletedEvent("09"));
        actionCompletedSink.tryEmitNext(buildActionCompletedEvent("10"));
        actionCompletedSink.tryEmitComplete();




        Thread.sleep(3000);

    }

    private ActionCompleted buildActionCompletedEvent(String requestId){
        return ActionCompleted.newBuilder()
                .setCorrelationId(requestId)
                .setRequestId(requestId)
                .setStatus("DEMO_SUCCESS").build();
    }

    private CorrelatedMessage<String, ActionCompleted> buildCorrelatedEvent(String requestId){
        return new CorrelatedMessage<>() {
            @Override
            public String getCorrelationId() {
                return requestId;
            }
            @Override
            public ActionCompleted getPayload() {
                return ActionCompleted.newBuilder()
                        .setCorrelationId(requestId)
                        .setRequestId(requestId)
                        .setStatus("DEMO_SUCCESS").build();
            }
            public String toString(){
                return String.format("{correlationId: %s, status: DEMO_SUCCESS}", requestId);
            }
        };
    }

    @Test
    public void replyingSubscriberTest() throws InterruptedException, ExecutionException, TimeoutException {

        Sinks.Many<CorrelatedMessage<String, ActionCompleted>> actionCompletedSink = Sinks.many()
                .multicast()
                .onBackpressureBuffer();

        Flux<CorrelatedMessage<String, ActionCompleted>> actionCompletedFlux = actionCompletedSink.asFlux()
                .doOnNext(actionCompleted -> System.out.println(
                        "[DEBUG] (" + Thread.currentThread().getName() + ") Emitted event with requestId:" +
                                actionCompleted.getPayload().getRequestId()
                ))
                .doOnComplete(
                        () -> System.out.println(
                                "[DEBUG] (" + Thread.currentThread().getName() + ") Flux completed:"
                        ))
                ;

        ReplyingSubscriber<String, ActionCompleted> subscriber =
                new ReplyingSubscriber<>("000", Duration.ofSeconds(2));



        CompletableFuture.runAsync(() ->
                {
                    try {
                        TimeUnit.SECONDS.sleep(3);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    actionCompletedSink.tryEmitNext(buildCorrelatedEvent("000"));
                }
        );

        actionCompletedFlux
                .filter(actionCompleted -> actionCompleted.getPayload().getRequestId().equals("000"))
                .flatMap(Mono::just)
                .subscribe(subscriber);

        System.out.println(subscriber.getResult());

        Thread.sleep(3000);

    }

}
