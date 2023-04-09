package dev.siriuz.kafka.streams;

import dev.siriuz.kafka.streams.domain.Transaction;
import dev.siriuz.kafka.streams.utils.Utils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class JsonParserTest {

    @Test
    public void transactionParserTest() {

        String jsonValue = "{\"name\"=\"Oliver\", \"amount\"=-821, \"timestamp\"=\"2023-02-14T22:26:27.727868800\"}";

        Transaction transaction = Utils.jsonToTransaction(jsonValue);
        System.out.println(transaction);

        Assertions.assertEquals(transaction.getAmount(), -821);

    }
}
