package dev.siriuz.kafkaspringdemo.domain.port;

import java.util.concurrent.CompletableFuture;

public interface RequestResponseProcessor<REQ, RES> {

    RES process(REQ request);
    CompletableFuture<RES> processAsync(REQ request);

}
