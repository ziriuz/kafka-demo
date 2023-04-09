package dev.siriuz.kafka.streams;

import dev.siriuz.kafka.streams.domain.Transaction;
import dev.siriuz.kafka.streams.service.TransactionService;
import dev.siriuz.model.BankTransaction;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


public class TransactionGeneratorApp {

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

    private TransactionService service;
    private Producer<String, BankTransaction> producer;

    public TransactionGeneratorApp(){
        producer = kafkaAvroProducer();
        service = new TransactionService(producer);
    }

    public void run() throws InterruptedException {


        Runtime.getRuntime().addShutdownHook(new Thread(() -> producer.close()));
        CountDownLatch latch = new CountDownLatch(1);


        transactionPublisher()
                .subscribeWith(
                        new Subscriber<BankTransaction>() {
                            Subscription sub;
                            @Override
                            public void onSubscribe(Subscription subscription) {
                                sub = subscription;
                                subscription.request(Long.MAX_VALUE);
                            }

                            @Override
                            public void onNext(BankTransaction transaction) {
                                System.out.println("+++++ " + transaction);
                                service.sendToKafka(transaction);
                                if (transaction.getAmount() < -999 ) {
                                    System.out.println("+++++ Cancel");
                                    sub.cancel();
                                    latch.countDown();
                                }
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

        TransactionGeneratorApp app = new TransactionGeneratorApp();
        try {
            app.run();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    private Flux<BankTransaction> transactionPublisher(){
        return Flux.interval(Duration.ofMillis(1000))
                .map(t -> service.createFakeTransaction());
    }

}
