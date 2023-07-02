package dev.siriuz.kafkaspringdemo.config;


import dev.siriuz.kafkaspringdemo.service.CorrelatedMessage;
import dev.siriuz.model.ActionCompleted;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

@Configuration
@Slf4j
public class ActionProcessorConfig {

    @Bean
    public Sinks.Many<ActionCompleted> actionCompletedSink() {
        //return Sinks.many().multicast().onBackpressureBuffer();
        return Sinks.many().replay().limit(1);
    }

    @Bean
    public Flux<ActionCompleted> streamActionCompleted(Sinks.Many<ActionCompleted> actionCompletedSink) {
        return actionCompletedSink.asFlux()
                .log()
                .doOnNext(actionCompleted ->
                        log.debug("Emitted event with requestId: {}", actionCompleted.getRequestId()))
                .doOnComplete(() -> log.debug("Flux<ActionCompleted> process was completed"))
        ;
    }

    @Bean
    Sinks.Many<CorrelatedMessage<String, ActionCompleted>> actionCompletedEventSink(){
        return Sinks.many().replay().limit(1);
    }

    @Bean
    Flux<CorrelatedMessage<String, ActionCompleted>> actionCompletedEventStream(
            Sinks.Many<CorrelatedMessage<String, ActionCompleted>> actionCompletedEventSink)
    {
        return actionCompletedEventSink.asFlux()
                .doOnNext( event ->
                        log.debug("Emitted event with correlationId: {}", event.getCorrelationId())
                )
                .doOnComplete(() -> log.debug("ActionCompleted Event Stream is completed"));
    }

}
