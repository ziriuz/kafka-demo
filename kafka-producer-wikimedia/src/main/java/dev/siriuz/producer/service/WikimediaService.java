package dev.siriuz.producer.service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

@Service
public class WikimediaService {

    @Autowired
    private WebClient wikimediaWebClient;

    public Flux<String> changesStream(){
        return wikimediaWebClient.get()
                .uri("v2/stream/recentchange")
                .retrieve()
                .bodyToFlux(String.class)
                ;
    }

}
