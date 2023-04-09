package dev.siriuz.kafka.streams.service;

import dev.siriuz.kafka.streams.domain.Transaction;
import dev.siriuz.kafka.streams.utils.Utils;
import dev.siriuz.model.BankTransaction;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class TransactionService {

    private Producer<String, BankTransaction> kafkaProducer;

    public TransactionService(Producer<String, BankTransaction>  kafkaProducer){
        this.kafkaProducer = kafkaProducer;
    }

    public BankTransaction createFakeTransaction(){
        return BankTransaction.newBuilder()
                .setAccount(Utils.randomUsername())
                .setAmount((int) (2000 * (Math.random() - 0.5)))
                .setTimestamp((int) LocalDateTime.now().toEpochSecond(ZoneOffset.UTC))
                .build();
    }

    public void sendToKafka(BankTransaction transaction){
        kafkaProducer.send(
                new ProducerRecord<>("bank-transactions",
                        transaction.getAccount(), transaction)
        );
    }

}
