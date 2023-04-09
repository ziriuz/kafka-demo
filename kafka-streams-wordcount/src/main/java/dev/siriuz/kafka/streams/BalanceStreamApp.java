package dev.siriuz.kafka.streams;

import dev.siriuz.kafka.streams.domain.Balance;
import dev.siriuz.kafka.streams.domain.Transaction;
import dev.siriuz.kafka.streams.utils.Utils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

public class BalanceStreamApp {
    private static Properties streamsConfig(){
        Properties streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "balance-streams-app");
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,2000);
        streamsConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,StreamsConfig.EXACTLY_ONCE_V2);
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, "D:/tmp/kafka-streams");
        return streamsConfig;
    }

    private static Topology streamsTopology(){
        StreamsBuilder builder = new StreamsBuilder();


        KStream<String, String> inputStream = builder.stream("bank-transactions");

        Serializer<Balance> balanceSerializer = (s, balance) -> Utils.balanceToJson(balance).getBytes(StandardCharsets.UTF_8);
        Deserializer<Balance> balanceDeserializer = (s, bytes) -> Utils.jsonToBalance(new String(bytes,StandardCharsets.UTF_8));
        Serde<Balance> balanceSerde = Serdes.serdeFrom(balanceSerializer, balanceDeserializer);

        KTable<String, Balance> balanceTable = inputStream
                .mapValues(Utils::jsonToTransaction)
                .peek((name, transaction) -> System.out.println("+++++ " + name + " " + transaction.getAmount()))
                .groupByKey()
                .aggregate(
                        () -> new Balance(0L, LocalDateTime.of(1970,1,1,0,0,0)),
                        (name, transaction, balance) -> {
                            balance.add(transaction.getAmount());
                            balance.modifyLastUpdate(transaction.getTimestamp());
                            return balance;
                        },Materialized.with(Serdes.String(), balanceSerde)
                );

        balanceTable
                .toStream()
                .peek((name, balance) -> System.out.println("***** " + name + ":" + balance))
                .to("bank-balance");

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
        startStreamsProcess(streamsTopology());
    }
}
