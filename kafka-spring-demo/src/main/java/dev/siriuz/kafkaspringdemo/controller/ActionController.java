package dev.siriuz.kafkaspringdemo.controller;

import dev.siriuz.kafkaspringdemo.domain.service.LifecycleService;
import dev.siriuz.kafkaspringdemo.dto.ActionCompletedResponse;
import dev.siriuz.kafkaspringdemo.dto.ActionRequest;
import dev.siriuz.kafkaspringdemo.service.ActionProcessorSpringReplyingTemplate;
import dev.siriuz.kafkaspringdemo.service.ActionRequestV2Service;
import dev.siriuz.kafkaspringdemo.util.DtoUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ActionController {

    @Autowired
    ActionProcessorSpringReplyingTemplate service;

    @Autowired
    ActionRequestV2Service actionProcessor;

    @Autowired
    LifecycleService lifecycleService;
    @PostMapping("action") //spring replying template
    public ActionCompletedResponse actionSpringReply(@RequestBody ActionRequest request){

        return DtoUtils.toDto(
                service.sendActionRequest(request.getRequestId(), DtoUtils.toAvro(request))
        );

    }

    @PostMapping("do") // custom replying template
    public ActionCompletedResponse actionCustomReply(@RequestBody ActionRequest request){

        return DtoUtils.toDto(
                actionProcessor.processActionRequest(request.getRequestId(), DtoUtils.toAvro(request))
        );

    }

    @PostMapping("react")
    public ActionCompletedResponse actionReactiveReply(@RequestBody ActionRequest request){

        return DtoUtils.toDto(
                lifecycleService.action(DtoUtils.toEntity(request))
        );

    }

}
