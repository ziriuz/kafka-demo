package dev.siriuz.producer;

import dev.siriuz.producer.service.WikimediaService;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class WikimediaChangesWorker implements CommandLineRunner {

    private final Logger logger = LoggerFactory.getLogger(WikimediaChangesWorker.class);

    @Autowired
    WikimediaService wikimediaService;

    @Autowired
    Subscriber<String> wikimediaChangesProducer;

    @Override
    public void run(String... args) {
        logger.info("WikimediaChangesWorker started...");
        wikimediaService.changesStream()
                .subscribe(wikimediaChangesProducer);

        // SpringBoot has shutdown hook which will close all Closeable components
        //Runtime.getRuntime().addShutdownHook(new Thread(wikimediaChangesProducer::stop));
    }
}
