package dev.siriuz.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;

import java.util.Arrays;
import java.util.Properties;

public class StreamsMockApp {


    private static Properties streamsConfig(){
        Properties streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-mock-app");
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,1000);
        streamsConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,StreamsConfig.EXACTLY_ONCE_V2);
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, "D:/tmp/kafka-streams");
        return streamsConfig;
    }

    private static Topology createStreamsTopology(){
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> inputStream = builder.stream("test.requested");

        inputStream.mapValues(value -> value.toLowerCase())
                .peek((key, value) -> System.out.println("sending: " + key + ":" + value))
                .to("test.provided")
                ;

        /*KTable<String, Long> wordCounts = inputStream
                .mapValues(value -> value.toLowerCase())
                .flatMapValues(value -> Arrays.asList(value.split(" ")))
                .selectKey((nullKey, word) -> word)
                .groupByKey()
                .count(Named.as("word-count-table"));

        wordCounts.toStream().to("test.provided");*/

        return builder.build();
    }

    private static void startStreamsProcess(Topology topology){

        System.out.println("====== Topology ==========================================");
        System.out.println(topology.describe());
        System.out.println("==========================================================");

        KafkaStreams streams = new KafkaStreams(topology, streamsConfig());

        streams.cleanUp();

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public static void main(String[] args) {
        startStreamsProcess(createStreamsTopology());
    }

}
