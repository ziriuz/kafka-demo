package dev.siriuz.producer.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

@Configuration
public class WebClientConfig {
    @Bean
    public WebClient wikimediaWebClient(){
        return WebClient.builder()
                .baseUrl("https://stream.wikimedia.org")
                .build();
    }
}
