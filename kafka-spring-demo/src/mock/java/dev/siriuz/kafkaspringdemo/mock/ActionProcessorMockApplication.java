package dev.siriuz.kafkaspringdemo.mock;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class ActionProcessorMockApplication {

    public static void main(String[] args) {
        SpringApplication.run(ActionProcessorMockApplication.class, args);
    }

}
