package dev.siriuz.kafka.streams;

import dev.siriuz.kafka.streams.service.TransactionService;
import dev.siriuz.kafka.streams.utils.Utils;
import dev.siriuz.model.BankTransaction;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


public class ProducerMockApp {

    public static Producer<String, String> kafkaProducer(){

        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.BATCH_SIZE_CONFIG,"3");
        return new KafkaProducer<>(config);

    }

    public static Producer<String, BankTransaction> kafkaAvroProducer(){
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        config.put("schema.registry.url", "http://127.0.0.1:8081");
        config.put(ProducerConfig.BATCH_SIZE_CONFIG,"3");
        return new KafkaProducer<>(config);
    }

    private Producer<String, String> producer;

    public ProducerMockApp(){
        producer = kafkaProducer();
    }

    public void run() throws InterruptedException {


        Runtime.getRuntime().addShutdownHook(new Thread(() -> producer.close()));
        CountDownLatch latch = new CountDownLatch(1);


        transactionPublisher()
                .subscribeWith(
                        new Subscriber<String>() {
                            Subscription sub;
                            @Override
                            public void onSubscribe(Subscription subscription) {
                                sub = subscription;
                                subscription.request(Long.MAX_VALUE);
                            }

                            @Override
                            public void onNext(String message) {
                                System.out.println("+++++ " + message);
                                producer.send(
                                        new ProducerRecord<>("test.requested",
                                                Utils.randomUsername(), message)
                                );
                            }

                            @Override
                            public void onError(Throwable throwable) {
                                System.out.println("+++++ ERROR: " + throwable.getMessage());
                                latch.countDown();
                            }

                            @Override
                            public void onComplete() {
                                System.out.println("+++++ Done");
                                latch.countDown();
                            }
                        }
                );
        latch.await();
        producer.close();
    }

    public static void main(String[] args) {

        ProducerMockApp app = new ProducerMockApp();
        try {
            app.run();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    private Flux<String> transactionPublisher(){
        return Flux.interval(Duration.ofMillis(500))
                .map(t -> "{\"t\":\"" + t + "\", \"partner_name\":\""+ Utils.randomUsername()+"\"}");
    }

}
