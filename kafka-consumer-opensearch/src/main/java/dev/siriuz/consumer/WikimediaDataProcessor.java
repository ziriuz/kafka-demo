package dev.siriuz.consumer;

import dev.siriuz.consumer.service.OpenSearchService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Component
public class WikimediaDataProcessor implements CommandLineRunner {

    private final Logger logger = LoggerFactory.getLogger(WikimediaDataProcessor.class);

    private final Executor executor = Executors.newFixedThreadPool(4);

    @Autowired
    private OpenSearchService openSearchService;

    @Autowired
    private Consumer<String, String> wikimediaKafkaConsumer;

    @Value("${kafka.topic.wikimedia}")
    private String topic;
    @Value("${opensearch.index.wikimedia}")
    private String index;

    private int i = 0;

    @Override
    public void run(String... args) throws IOException {
        logger.info("Creating opensearch index if not exists");
        openSearchService.createIndex(index);
        logger.info("Kafka consumer subscribed to " + topic);
        wikimediaKafkaConsumer.subscribe(Collections.singleton(topic));

        while(true) {
            ConsumerRecords<String, String> records = wikimediaKafkaConsumer.poll(Duration.ofSeconds(1));
            int recordCount = records.count();
            logger.info("Received " + recordCount + " records from kafka wikimedia");
            if (recordCount>0) i++;
            for (ConsumerRecord<String, String> record:records){
                openSearchService.bulkInsert(index, record.value(), XContentType.JSON);
                //executor.execute(
                //        () -> openSearchService.insert(index, record.value(), XContentType.JSON));
            }
            openSearchService.executeBulkRequest();
            // If autocommit is disabled, manually commit offsets
            // But it should be done only after successful processing data
            // What to do if processing one of messages failed?
            wikimediaKafkaConsumer.commitSync();
            try {
                // to increase size of butch for opensearch
                TimeUnit.SECONDS.sleep(2);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            logger.info("Offsets have been committed");
        }
    }
}
