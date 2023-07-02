package dev.siriuz.gatling;

import io.gatling.javaapi.core.ActionBuilder;
import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import ru.tinkoff.gatling.kafka.javaapi.protocol.KafkaProtocolBuilder;
import ru.tinkoff.gatling.kafka.javaapi.protocol.KafkaProtocolBuilderNew;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.clients.producer.ProducerConfig;
import static io.gatling.javaapi.core.CoreDsl.*;
import static ru.tinkoff.gatling.kafka.javaapi.KafkaDsl.kafka;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class BasicKafkaSimulation {//extends Simulation {
    private final KafkaProtocolBuilder kafkaConf = kafka()
            .topic("test.requested")
            .properties(Map.of(ProducerConfig.ACKS_CONFIG, "1"));

    private final KafkaProtocolBuilderNew kafkaProtocol = kafka().requestReply()
            .producerSettings(
                    Map.of(
                            ProducerConfig.ACKS_CONFIG, "1",
                            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"
                    )
            )
            .consumeSettings(
                    Map.of("bootstrap.servers", "localhost:9092")
            ).timeout(Duration.ofSeconds(5));

    private final AtomicInteger c = new AtomicInteger(0);

    private final Iterator<Map<String, Object>> feeder =
            Stream.generate((Supplier<Map<String, Object>>) () -> Collections.singletonMap("kekey", c.incrementAndGet())
            ).iterator();

    private final Headers headers = new RecordHeaders().add("test-header", "test_value".getBytes());

    private final ScenarioBuilder scn = scenario("Kafka request reply scenario")
            .feed(feeder)
            .exec(
                    kafka("ReqRep").requestReply()
                            .requestTopic("test.requested")
                            .replyTopic("test.provided")
                            .send("#{kekey}", """
                            { "m": "DkF" }
                            """, headers, String.class, String.class)
                            .check(jsonPath("$.m").is("dkf"))
            )
            .exec( session -> {
                System.out.println("================== "+"#{kekey}");
                System.out.println(session);
                System.out.println("==================");
                return session;
            });

//    {
//        setUp( scn.injectOpen(
//                        constantUsersPerSec(5).during(Duration.ofSeconds(2)),
//                        rampUsersPerSec(10).to(20).during(Duration.ofSeconds(2)).randomized()
//                    )
//             )
//                //.normalPausesWithStdDevDuration(Duration.ofMillis(500))
//                .protocols(kafkaProtocol)
//                .maxDuration(Duration.ofSeconds(5));
//    }
}
