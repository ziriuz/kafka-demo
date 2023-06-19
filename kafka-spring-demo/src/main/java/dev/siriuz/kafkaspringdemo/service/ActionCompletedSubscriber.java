package dev.siriuz.kafkaspringdemo.service;

import dev.siriuz.model.ActionCompleted;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
public class ActionCompletedSubscriber implements Subscriber<ActionCompleted> {
    private Subscription subscription;
    private String correlationId;

    private final int DEFAULT_TIMEOUT_MILLIS = 5000;

    private long timeout;
    private long startTime;
    private long completionTime;

    private CompletableFuture<ActionCompleted> actionCompletedFuture;

    private ActionCompletedSubscriber() {
    }

    public ActionCompletedSubscriber(String correlationId) {
        this.correlationId = correlationId;
        this.timeout = DEFAULT_TIMEOUT_MILLIS;
    }

    public ActionCompletedSubscriber(String correlationId, long timeout) {
        this.correlationId = correlationId;
        this.timeout = timeout;
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1);//Long.MAX_VALUE);
        System.out.println("[DEBUG] (" + Thread.currentThread().getName() + ") " + correlationId + ": in onSubscribe()");
        this.actionCompletedFuture = new CompletableFuture<>();
        startTime = Instant.now().toEpochMilli();
    }

    @Override
    public void onNext(ActionCompleted event) {
        System.out.println("[DEBUG] (" + Thread.currentThread().getName() + ") " + correlationId + ": subscriber received: " + event);
        if( event.getCorrelationId().equals(this.correlationId) ){
            System.out.println("[INFO] (" + Thread.currentThread().getName() + ") " + correlationId + " Action completed with status: " + event.getStatus());
            subscription.cancel();
            System.out.println("[DEBUG] (" + Thread.currentThread().getName() + ") " + correlationId + " Subscription canceled as expected result received from stream");
            this.actionCompletedFuture.complete(event);
            completionTime = Instant.now().toEpochMilli();
        } else {
            throw new RuntimeException(this.correlationId + " expected, but " + event.getCorrelationId() + " received");
        }
    }

    @Override
    public void onError(Throwable throwable) {
        System.out.println(correlationId + " subscriber ERROR: " + throwable.getMessage());
    }

    @Override
    public void onComplete() {
        System.out.println("[INFO] (" + Thread.currentThread().getName() + ") " + correlationId + " in onComplete()");
    }

    public void request(){
        subscription.request(1);
    }
    public void request(int num){
        subscription.request(num);
    }
    public ActionCompleted getResult() throws TimeoutException {
        ActionCompleted event;
        try {
            log.info("{}: in getResult(), timeout {} ms", correlationId, timeout);
            event = this.actionCompletedFuture.get(timeout, TimeUnit.MILLISECONDS);
            log.info("request {}: request completed within {} ms", correlationId, completionTime - startTime);
        } catch (InterruptedException e) {
            log.error("request {}: Interrupted", correlationId);
            e.printStackTrace();
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            log.error("request {}: failed: {}", correlationId, e.getMessage());
            e.printStackTrace();
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            log.error("request {}: not completed: {}", correlationId, e.getMessage());
            e.printStackTrace();
            subscription.cancel();
            log.info("request {}: {} ms passed after submission", correlationId, Instant.now().toEpochMilli() - startTime);
            log.info("request {}: cancelled due to {} ms timeout exceeded", correlationId, timeout);
            throw e;
        }
        return event;
    }
}
