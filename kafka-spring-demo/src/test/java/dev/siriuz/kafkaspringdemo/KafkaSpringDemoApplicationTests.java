package dev.siriuz.kafkaspringdemo;

import dev.siriuz.kafkaspringdemo.service.ActionRequestService;
import dev.siriuz.model.ActionCompleted;
import dev.siriuz.model.ActionRequested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.UUID;

@SpringBootTest
class KafkaSpringDemoApplicationTests {

    @Autowired
    ActionRequestService underTest;
    @Test
    void contextLoads() {
    }

    @Test
    public void actionRequestReplyTest() throws Exception {
        String requestId = UUID.randomUUID().toString();

        ActionRequested requestEvent = ActionRequested.newBuilder()
                .setRequestId(requestId)
                .setPayload("Request to perform some action")
                .build();

        ActionCompleted result = underTest.sendActionRequest(requestId, requestEvent);

        System.out.println(result);

        assert(result.getRequestId().equals(requestId));

    }

}
