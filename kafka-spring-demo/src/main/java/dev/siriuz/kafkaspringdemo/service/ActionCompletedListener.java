package dev.siriuz.kafkaspringdemo.service;


import dev.siriuz.kafkaspringdemo.config.KafkaSpringConfig;
import dev.siriuz.model.ActionCompleted;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import reactor.core.publisher.Sinks;

@Slf4j
public class ActionCompletedListener {

    private final String ID = "action-completed-listener";

    @Autowired
    Sinks.Many<ActionCompleted> actionCompletedSink;

    @KafkaListener(id = ID, topics = "${kafka.topic.action.completed}", groupId = "${kafka.consumer.id}",
            containerFactory = KafkaSpringConfig.LISTENER_CONTAINER_FACTORY)
    public void listenActionCompleted(ConsumerRecord<String, ActionCompleted> record) {
        log.info("{} consumed message -> {} -> {}",ID, record.key(), record.value());
        actionCompletedSink.tryEmitNext(record.value());
    }

}
