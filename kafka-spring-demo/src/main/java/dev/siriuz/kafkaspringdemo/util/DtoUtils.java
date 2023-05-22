package dev.siriuz.kafkaspringdemo.util;

import dev.siriuz.kafkaspringdemo.dto.ActionCompletedResponse;
import dev.siriuz.kafkaspringdemo.dto.ActionRequest;
import dev.siriuz.model.ActionCompleted;
import dev.siriuz.model.ActionRequested;

public class DtoUtils {

    public static ActionRequested toAvro(ActionRequest request){
        return ActionRequested.newBuilder()
                .setRequestId(request.getRequestId())
                .setPayload(request.getPayload())
                .build();
    }

    public static ActionCompletedResponse toTdo(ActionCompleted event){
        return new ActionCompletedResponse(
                event.getRequestId(),
                event.getStatus()
        );
    }

}
