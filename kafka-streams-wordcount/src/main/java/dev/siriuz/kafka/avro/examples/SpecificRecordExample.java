package dev.siriuz.kafka.avro.examples;

import dev.siriuz.kafka.utils.AvroUtils;
import dev.siriuz.model.BankBalance;
import dev.siriuz.model.Customer;
import dev.siriuz.model.EventHeader;
import org.apache.avro.AvroTypeException;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Date;

public class SpecificRecordExample {
    public static void main(String[] args) {

        Path path = Paths.get("kafka-streams-wordcount","sample_events");
        File f = new File(path.toFile(),"Balance.json");

        final String jsonFileName = "kafka-streams-wordcount/sample_events/Balance.json";

        BankBalance balance = BankBalance.newBuilder()
                .setAccount("123412341234")
                .setAmount(100)
                .setTimestamp(Instant.now().toEpochMilli())
                .setEventHeader(
                        EventHeader.newBuilder()
                                .setSendTime(Instant.now().toEpochMilli())
                                .setSource("Producer 01")
                                .setInfo("json validation against avro schema sample")
                                .build()
                )
                .build();

        AvroUtils.writeJsonFile(balance, jsonFileName, BankBalance.getClassSchema());
        System.out.printf("Written to json %s\n", jsonFileName);

        try {
            System.out.printf("Reading from json %s\n", jsonFileName);
            BankBalance balance2 = (BankBalance) AvroUtils.readJsonFile(jsonFileName, BankBalance.getClassSchema());
            System.out.println(balance2);
        }
        catch (AvroTypeException e){
            e.printStackTrace();
        }

        // 0: Build customer (specific record)
        Customer.Builder customerBuilder = Customer.newBuilder();
        Customer customer = customerBuilder
                .setFirstName("John")
                .setLastName("Doe")
                .setAge(23)
                .setHeight(175f)
                .setWeight(76.4f)
                .build();

        System.out.println(customer);

        // 1: Write Customer object to file
        final DatumWriter<Customer> datumWriter = new SpecificDatumWriter<>(Customer.class);

        try (DataFileWriter<Customer> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(customer.getSchema(), new File(path.toFile(), "customer-specific.avro"));
            dataFileWriter.append(customer);
            System.out.println("successfully wrote customer-specific.avro");
        } catch (IOException e){
            e.printStackTrace();
        }

        // 2: Read Customer object from file
        final File file = new File(path.toFile(), "customer-specific.avro");
        final DatumReader<Customer> datumReader = new SpecificDatumReader<>(Customer.class);
        try (DataFileReader<Customer> dataFileReader = new DataFileReader<>(file, datumReader)) {
            System.out.println("Reading our specific record");
            while (dataFileReader.hasNext()) {
                Customer readCustomer = dataFileReader.next();
                System.out.println(readCustomer.toString());
                System.out.println("First name: " + readCustomer.getFirstName());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
