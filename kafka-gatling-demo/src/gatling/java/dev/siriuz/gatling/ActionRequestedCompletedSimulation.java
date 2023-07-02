package dev.siriuz.gatling;

import io.gatling.javaapi.core.ChainBuilder;
import io.gatling.javaapi.core.FeederBuilder;
import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import io.gatling.javaapi.http.HttpProtocolBuilder;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.gatling.javaapi.core.CoreDsl.*;
import static io.gatling.javaapi.http.HttpDsl.http;
import static io.gatling.javaapi.http.HttpDsl.status;

public class ActionRequestedCompletedSimulation extends Simulation {

    private final Iterator<Map<String, Object>> feeder =
            Stream.generate((Supplier<Map<String, Object>>) () -> Collections.singletonMap("request_id", UUID.randomUUID().toString().replace("-",""))
            ).iterator();

    String bodyJsonTemplate = """
            {"requestId":"#{request_id}",
             "payload":"Demo-action-#{request_id}"}
            """;
    ChainBuilder actionApiCall =
        // let's try at max 2 times
        tryMax(2)
            .on(feed(feeder)
                    .exec(
                        http("ActionRequest")
                            .post("/react")
                                .asJson()
                                .body(StringBody(bodyJsonTemplate))
                            .check(
                                status().is(
                                    // we do a check on a condition that's been customized with
                                    // a lambda. It will be evaluated every time a user executes
                                    // the request
                                    session -> 200
                                )
                            )
                            .check(
                                    jsonPath("$.requestId").is(session -> session.get("request_id"))
                            )
                    )
//                    .exec( session -> {
//                        System.out.println("================== #{request_id}");
//                        System.out.println((String) session.get("request_id"));
//                        System.out.println("==================");
//                        return session;
//                    })
            )
            // if the chain didn't finally succeed, have the user exit the whole scenario
            .exitHereIfFailed();

    HttpProtocolBuilder httpProtocol =
        http.baseUrl("http://localhost:8080");

    ScenarioBuilder actionApi = scenario("Action request API test").exec(actionApiCall);

    {
        setUp(
                actionApi.injectOpen(
                        rampUsersPerSec(110).to(120).during(Duration.ofSeconds(20)).randomized()
                )
        ).protocols(httpProtocol);
    }
}