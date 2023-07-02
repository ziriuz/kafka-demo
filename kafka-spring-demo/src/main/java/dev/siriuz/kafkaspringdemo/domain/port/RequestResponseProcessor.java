package dev.siriuz.kafkaspringdemo.domain.port;

import java.util.concurrent.CompletableFuture;

public interface RequestResponseProcessor<REQ, RES> {

    public RES process(REQ request);
    public CompletableFuture<RES> processAsync(REQ request);

}
