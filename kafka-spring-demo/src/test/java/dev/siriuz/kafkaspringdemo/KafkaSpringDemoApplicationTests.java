package dev.siriuz.kafkaspringdemo;

import dev.siriuz.kafkaspringdemo.service.ActionRequestReactiveService;
import dev.siriuz.kafkaspringdemo.service.ActionRequestService;
import dev.siriuz.model.ActionCompleted;
import dev.siriuz.model.ActionRequested;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.UUID;

@SpringBootTest
class KafkaSpringDemoApplicationTests {

    @Autowired
    ActionRequestService underTest;

    @Autowired
    ActionRequestReactiveService reactiveService;

    @Test
    void contextLoads() {
    }

    @Test
    @Disabled
    public void springRequestReplyTest() {
        String requestId = UUID.randomUUID().toString();

        ActionRequested requestEvent = ActionRequested.newBuilder()
                .setCorrelationId(requestId)
                .setRequestId(requestId)
                .setPayload("Request to perform some action")
                .build();

        ActionCompleted result = underTest.sendActionRequest(requestId, requestEvent);

        System.out.println(result);

        assert(result.getRequestId().equals(requestId));

    }

    @Test
    @Disabled
    public void customRequestResponseTest() {
        String requestId = UUID.randomUUID().toString();

        ActionRequested requestEvent = ActionRequested.newBuilder()
                .setCorrelationId("0")
                .setRequestId(requestId)
                .setPayload("Request to perform some action")
                .build();

        ActionCompleted result = reactiveService.processActionRequest(requestId, requestEvent);

        System.out.println(result);

        assert(result.getRequestId().equals(requestId));

    }
}
