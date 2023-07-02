package dev.siriuz.kafkaspringdemo;

import dev.siriuz.kafkaspringdemo.domain.model.ActionResult;
import dev.siriuz.kafkaspringdemo.domain.model.DomainActionRequest;
import dev.siriuz.kafkaspringdemo.service.ActionProcessorReactive;
import dev.siriuz.kafkaspringdemo.service.ActionProcessorSpringReplyingTemplate;
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
    ActionProcessorSpringReplyingTemplate underTest;

    @Autowired
    ActionProcessorReactive reactiveService;

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

        DomainActionRequest request = new DomainActionRequest(requestId,"Request to perform some action");

        ActionResult result = reactiveService.process(request);

        System.out.println(result);

        assert(result.getRequestId().equals(requestId));

    }
}
