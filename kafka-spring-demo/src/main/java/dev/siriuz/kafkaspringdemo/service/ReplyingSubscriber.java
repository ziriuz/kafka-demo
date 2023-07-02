package dev.siriuz.kafkaspringdemo.service;

import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

@Slf4j
public class ReplyingSubscriber<C, E, R> implements Subscriber<CorrelatedMessage<C, E>> {
    private Subscription subscription;
    private C correlationId;
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(5);
    private Duration timeout;
    private long startTime;
    private long completionTime;

    private CompletableFuture<E> replyFuture;
    private CompletableFuture<R> responseFuture;
    private Function<E, R> convert;

    private ReplyingSubscriber() {
    }
    public ReplyingSubscriber(C correlationId, Function<E, R> convert) {
        this(correlationId, convert, DEFAULT_TIMEOUT);
    }
    public ReplyingSubscriber(C correlationId, Function<E, R> convert, Duration timeout) {
        this.correlationId = correlationId;
        this.convert = convert;
        this.timeout = timeout;
    }
    @Override
    public void onSubscribe(Subscription subscription) {
        log.debug("<{}>: enter onSubscribe()", correlationId);
        this.subscription = subscription;
        subscription.request(1); //Flux should be filtered and mapped to Mono
        this.replyFuture = new CompletableFuture<>();
        this.responseFuture = new CompletableFuture<>();
        startTime = Instant.now().toEpochMilli();
    }

    @Override
    public void onNext(CorrelatedMessage<C, E> event) {

        if( event.getCorrelationId().equals(this.correlationId) ){
            log.debug("<{}> subscriber received: {}", correlationId, event.getPayload());
            subscription.cancel();
            log.debug("<{}> Subscription canceled as expected result received from stream", correlationId);
            this.replyFuture.complete(event.getPayload());
            this.responseFuture.complete(convert.apply(event.getPayload()));
            completionTime = Instant.now().toEpochMilli();
        } else {
            throw new RuntimeException(this.correlationId + " expected, but " + event.getCorrelationId() + " received. " + " correlationId filter might not be added to publisher pipeline");
        }
    }

    @Override
    public void onError(Throwable throwable) {
        log.error("<{}> subscriber ERROR: {}", correlationId, throwable.getMessage());
    }

    @Override
    public void onComplete() {
        log.error("<{}> publishing completed", correlationId);
    }

    public void request(){
        subscription.request(1);
    }
    public void request(int num){
        subscription.request(num);
    }

    // TODO: implement scheduler like in spring replying Template
    //       to handle situation when no one call getResult

    public CompletableFuture<R> getFuture() {
        return responseFuture;
    }

    public E getResult() throws TimeoutException {
        E event;
        try {
            log.info("<{}> enter getResult(), timeout is set to {} ms", correlationId, timeout.toMillis());
            event = replyFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            log.info("<{}> request completed within {} ms", correlationId, completionTime - startTime);
        } catch (InterruptedException e) {
            log.error("<{}>  Interrupted", correlationId);
            e.printStackTrace();
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            log.error("<{}> failed: {}", correlationId, e.getMessage());
            e.printStackTrace();
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            log.error("<{}> not completed: {}", correlationId, e.getMessage());
            e.printStackTrace();
            subscription.cancel();
            log.info("<{}> {} ms passed after submission", correlationId, Instant.now().toEpochMilli() - startTime);
            log.info("<{}> subscription cancelled due to {} ms timeout exceeded", correlationId, timeout.toMillis());
            throw e;
        }
        return event;
    }

    public C getCorrelationId() {
        return correlationId;
    }
}
