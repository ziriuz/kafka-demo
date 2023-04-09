package dev.siriuz.producer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Closeable;

@Component
public class WikimediaChangesProducer implements Subscriber<String>, Closeable {

    private final Logger logger = LoggerFactory.getLogger(WikimediaChangesProducer.class);

    @Autowired
    private Producer<String, String> wikimediaProducer;

    private Subscription subscription;

    private final String topic;

    public WikimediaChangesProducer(String wikimediaTopic){
        this.topic = wikimediaTopic;
    }

    @Override
    public void onSubscribe(Subscription s) {
        subscription = s;
        subscription.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(String s) {
        logger.info("sending: " + s);
        wikimediaProducer.send(new ProducerRecord<>(topic,s));
    }

    @Override
    public void onError(Throwable t) {
        logger.error("Error in Wikimedia stream Producer", t);
    }

    @Override
    public void onComplete() {
        logger.info("Wikimedia Stream completed");
    }

    public void stop(){
        subscription.cancel();
        logger.info("Wikimedia subscription canceled");
        wikimediaProducer.close();
        logger.info("Kafka producer closed");
    }

    @Override
    public void close() {
        // Spring boot shutdown hook executes this
        subscription.cancel();
        logger.info("Wikimedia subscription canceled");
    }
}
