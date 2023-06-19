package dev.siriuz.kafkaspringdemo.mock.service;

import dev.siriuz.model.ActionCompleted;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class ActionCompletedProducerInterceptor implements ProducerInterceptor<String, ActionCompleted> {
    @Override
    public ProducerRecord<String, ActionCompleted> onSend(ProducerRecord<String, ActionCompleted> record) {

        System.out.println("======Producer interceptor======");
        record.headers().forEach(System.out::println);
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
