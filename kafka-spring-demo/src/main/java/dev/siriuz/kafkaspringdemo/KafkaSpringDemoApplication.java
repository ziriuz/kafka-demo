package dev.siriuz.kafkaspringdemo;

import dev.siriuz.model.BankTransaction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;

@SpringBootApplication
public class KafkaSpringDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaSpringDemoApplication.class, args);
    }

    @KafkaListener(id = "kafka-spring-demo-app", topics = "bank-transactions")
    public void listen(ConsumerRecord<String, BankTransaction> record) {
        System.out.println("----------------------->");
        System.out.printf("Consumed message -> %s -> %s\n", record.key(), record.value());
    }
}

