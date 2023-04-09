package dev.siriuz.kafka.utils;

import dev.siriuz.model.BankBalance;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.*;

public class AvroUtils {

    public static GenericRecord readAvroFileGeneric(String fileName, Schema avroSchema){
        final File file = new File(fileName);
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(avroSchema);
        GenericRecord record = null;
        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader)){
            record = dataFileReader.next();
        }
        catch(IOException e) {
            e.printStackTrace();
        }
        return record;
    }

    public static void writeAvroFileGeneric(GenericRecord record, String fileName){

        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(record.getSchema());

        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(record.getSchema(), new File(fileName));
            dataFileWriter.append(record);
            System.out.println("Written customer-generic.avro");
        } catch (IOException e) {
            System.out.println("Couldn't write file");
            e.printStackTrace();
        }
    }

    public static GenericRecord readJsonFileGeneric(String fileName, Schema schema){
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        GenericRecord record = null;
        try (InputStream inputStream = new FileInputStream(fileName)){
            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, inputStream);
            record = datumReader.read(null, decoder);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return record;
    }

    public static void writeJsonFileGeneric(GenericRecord record, String fileName){
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(record.getSchema());
        try {
            OutputStream stream = new FileOutputStream(fileName);
            Encoder encoder =  EncoderFactory.get().jsonEncoder(record.getSchema(), stream);
            datumWriter.write(record, encoder);
            encoder.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static BankBalance readJsonFile(String fileName){
        Schema avroSchema = BankBalance.getClassSchema();
        final DatumReader<BankBalance> datumReader = new SpecificDatumReader<>(BankBalance.getClassSchema());
        BankBalance record = null;
        try (InputStream inputStream = new FileInputStream(fileName)){
            Decoder decoder = DecoderFactory.get().jsonDecoder(avroSchema, inputStream);
            record = datumReader.read(null, decoder);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return record;
    }

    public static void writeJsonFile(BankBalance record, String fileName){
        Schema avroSchema = BankBalance.getClassSchema();
        final DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<>(avroSchema);
        try {
            OutputStream stream = new FileOutputStream(fileName);
            Encoder encoder =  EncoderFactory.get().jsonEncoder(avroSchema, stream);
            datumWriter.write(record, encoder);
            encoder.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Object readJsonFile(String fileName, Schema schema){

        final DatumReader<Object> datumReader = new SpecificDatumReader<>(schema);
        Object record = null;
        try (InputStream inputStream = new FileInputStream(fileName)){
            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, inputStream);
            record = datumReader.read(null, decoder);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return record;
    }
}
