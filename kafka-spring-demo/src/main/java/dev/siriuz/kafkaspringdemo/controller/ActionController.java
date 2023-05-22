package dev.siriuz.kafkaspringdemo.controller;

import dev.siriuz.kafkaspringdemo.dto.ActionCompletedResponse;
import dev.siriuz.kafkaspringdemo.dto.ActionRequest;
import dev.siriuz.kafkaspringdemo.service.ActionRequestService;
import dev.siriuz.kafkaspringdemo.util.DtoUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ActionController {

    @Autowired
    ActionRequestService service;

    @PostMapping("action")
    public ActionCompletedResponse action(@RequestBody ActionRequest request){

        return DtoUtils.toTdo(
                service.sendActionRequest(request.getRequestId(), DtoUtils.toAvro(request))
        );

    }


}
