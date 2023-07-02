package dev.siriuz.kafkaspringdemo.domain.service;

import dev.siriuz.kafkaspringdemo.domain.model.ActionResult;
import dev.siriuz.kafkaspringdemo.domain.model.DomainActionRequest;
import dev.siriuz.kafkaspringdemo.domain.port.RequestResponseProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class LifecycleService {

    @Autowired
    private RequestResponseProcessor<DomainActionRequest, ActionResult> actionService;

    public ActionResult action(DomainActionRequest request){

        return actionService.process(request);

    }



}
