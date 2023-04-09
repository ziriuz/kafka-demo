package dev.siriuz.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;

import java.util.Arrays;
import java.util.Properties;

public class StreamsStarterApp {


    private static Properties streamsConfig(){
        Properties streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-app");
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,2000);
        streamsConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,StreamsConfig.EXACTLY_ONCE_V2);
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, "D:/tmp/kafka-streams");
        return streamsConfig;
    }

    private static Topology createWordCountTopology(){
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> inputStream = builder.stream("word-count-input");

        KTable<String, Long> wordCounts = inputStream
                .mapValues(value -> value.toLowerCase())
                .flatMapValues(value -> Arrays.asList(value.split(" ")))
                .selectKey((nullKey, word) -> word)
                .groupByKey()
                .count(Named.as("word-count-table"));

        wordCounts.toStream().to("word-count-output");

        return builder.build();
    }

    private static Topology createColorCountTopology(){
        StreamsBuilder builder = new StreamsBuilder();


        //Read input topic as KTable
        KTable<String, String> inputTable = builder.table("color-count-input");
        KTable<String, String> helperTable = inputTable
                //convert values to lower case
                .mapValues(value -> value.toLowerCase())
                //filter colors
                .filter((name, color) -> color.equals("red")||color.equals("blue")||color.equals("green"));
                //set color as key
        helperTable.toStream().to("color-filtered");

        KTable<String, String> filteredTable = builder.table("color-filtered");
        KTable<String, Long> outputTable = filteredTable
                //.selectKey((name, color) -> color)
                //group by color
                .groupBy((name, color) -> KeyValue.pair(color,"1"))
                //count
                .count(Named.as("color-count-table"));

        outputTable.toStream().to("color-count-output");

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
        startStreamsProcess(createColorCountTopology());
    }

}
