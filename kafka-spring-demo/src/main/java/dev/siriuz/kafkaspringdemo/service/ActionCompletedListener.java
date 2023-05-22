package dev.siriuz.kafkaspringdemo.service;


import dev.siriuz.model.ActionCompleted;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;

public class ActionCompletedListener {

    private final String ID = "demo-action-completed-listener";

    @KafkaListener(id = ID, topics = "demo.action.completed")
    public void listenActionCompleted(ConsumerRecord<String, ActionCompleted> record) {
        System.out.printf("%s -> Consumed message -> %s -> %s\n",ID, record.key(), record.value());
    }

}
