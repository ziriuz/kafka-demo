package dev.siriuz.kafkaspringdemo.service;

public interface CorrelatedMessage<K, V> {

    K getCorrelationId();

    V getPayload();

}
