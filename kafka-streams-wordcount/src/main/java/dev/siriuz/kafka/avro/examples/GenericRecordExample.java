package dev.siriuz.kafka.avro.examples;

import dev.siriuz.kafka.utils.AvroUtils;
import dev.siriuz.model.Customer;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.*;
import org.apache.avro.io.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class GenericRecordExample {

    public static void main(String[] args) {


        // 0: Define schema
        Schema.Parser parser = new Schema.Parser();
        String schemaString = "{\n" +
                "     \"type\": \"record\",\n" +
                "     \"namespace\": \"com.example\",\n" +
                "     \"name\": \"Customer\",\n" +
                "     \"doc\": \"Avro Schema for our Customer\",     \n" +
                "     \"fields\": [\n" +
                "       { \"name\": \"first_name\", \"type\": \"string\", \"doc\": \"First Name of Customer\" },\n" +
                "       { \"name\": \"last_name\", \"type\": \"string\", \"doc\": \"Last Name of Customer\" },\n" +
                "       { \"name\": \"middle_name\", \"type\": [\"null\",\"string\"], \"default\":null, \"doc\": \"Middle Name of Customer\" },\n" +
                "       { \"name\": \"age\", \"type\": [\"null\",\"int\"], \"doc\": \"Age at the time of registration\" },\n" +
                "       { \"name\": \"height\", \"type\": \"float\", \"doc\": \"Height at the time of registration in cm\" },\n" +
                "       { \"name\": \"weight\", \"type\": \"float\", \"doc\": \"Weight at the time of registration in kg\" },\n" +
                "       { \"name\": \"automated_email\", \"type\": \"boolean\", \"default\": true, \"doc\": \"Field indicating if the user is enrolled in marketing emails\" }\n" +
                "     ]\n" +
                "}";
        Schema customerSchema = parser.parse(schemaString);


        // 1: Create Generic record
        GenericRecordBuilder customerBuilder = new GenericRecordBuilder(customerSchema);
        GenericData.Record customerRecord = customerBuilder
                .set("first_name", "John")
                .set("last_name", "Doe")
                .set("age", 44)
                .set("height", 180f)
                .set("weight", 88.3f)
                .set("automated_email", false)
                .build();

        System.out.println(customerRecord);

        // 2: Write generic record to file
        AvroUtils.writeAvroFileGeneric(customerRecord, "customer-generic.avro");

        // 3: Read generic record from file
        GenericRecord customerRead = AvroUtils.readAvroFileGeneric("customer-generic.avro", customerSchema);
        try {
            // 4: Interpret as a generic record
            // get the data from the generic record
            System.out.println("First name: " + customerRead.get("first_name"));
            // read a non-existent field
            System.out.println("Non existent field: " + customerRead.get("not_here"));
        }
        catch (AvroRuntimeException e){
            e.printStackTrace();
        }

        GenericRecord record = AvroUtils.readJsonFileGeneric("customer-generic.json", customerSchema);
        System.out.println("json read:");
        System.out.println(record);

        AvroUtils.writeJsonFileGeneric(record,"customer-generic-2.json");

    }
}
