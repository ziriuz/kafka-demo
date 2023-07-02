package dev.siriuz.kafkaspringdemo.util;

import dev.siriuz.kafkaspringdemo.domain.model.ActionResult;
import dev.siriuz.kafkaspringdemo.domain.model.DomainActionRequest;
import dev.siriuz.kafkaspringdemo.domain.model.LifecycleResult;
import dev.siriuz.kafkaspringdemo.dto.ActionCompletedResponse;
import dev.siriuz.kafkaspringdemo.dto.ActionRequest;
import dev.siriuz.model.ActionCompleted;
import dev.siriuz.model.ActionRequested;
import dev.siriuz.model.LifecycleCompleted;

public class DtoUtils {

    public static ActionRequested toAvro(ActionRequest request){
        return ActionRequested.newBuilder()
                .setRequestId(request.getRequestId())
                .setPayload(request.getPayload())
                .build();
    }

    public static ActionRequested toAvro(DomainActionRequest request){
        return ActionRequested.newBuilder()
                .setRequestId(request.getRequestId())
                .setPayload(request.getPayload())
                .build();
    }

    public static ActionCompletedResponse toDto(ActionCompleted event){
        return new ActionCompletedResponse(
                event.getRequestId(),
                event.getStatus()
        );
    }

    public static ActionCompletedResponse toDto(ActionResult entity){
        return new ActionCompletedResponse(
                entity.getRequestId(),
                entity.getStatus()
        );
    }
    public static ActionResult toEntity(ActionCompleted actionCompleted) {
        ActionResult result = new ActionResult();
        result.setRequestId(actionCompleted.getRequestId());
        result.setStatus(actionCompleted.getStatus());
        return result;
    }

    public static LifecycleResult toEntity(LifecycleCompleted lifecycleCompleted) {
        LifecycleResult result = new LifecycleResult();
        result.setRequestId(lifecycleCompleted.getRequestId());
        result.setStatus(lifecycleCompleted.getResult());
        return result;
    }

    public static DomainActionRequest toEntity(ActionRequest request) {
        return new DomainActionRequest(request.getRequestId(), request.getPayload());


    }
}
