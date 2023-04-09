package dev.siriuz.kafka.utils;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;

public class AvroUtils {

    private static final DatumReader<SpecificRecord> specificDatumReader = new SpecificDatumReader<>();
    private static final DatumWriter<SpecificRecord> specificDatumWriter = new SpecificDatumWriter<>();

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
        try (InputStream inputStream = Files.newInputStream(Paths.get(fileName))){
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
            OutputStream stream = Files.newOutputStream(Paths.get(fileName));
            Encoder encoder =  EncoderFactory.get().jsonEncoder(record.getSchema(), stream);
            datumWriter.write(record, encoder);
            encoder.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Object readJsonFile(String fileName, Schema schema){
        specificDatumReader.setSchema(schema);
        SpecificRecord record = null;
        try (InputStream inputStream = Files.newInputStream(Paths.get(fileName))){
            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, inputStream);
            record = specificDatumReader.read(null, decoder);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return record;
    }

    public static void writeJsonFile(SpecificRecord record, String fileName, Schema schema){
        specificDatumWriter.setSchema(schema);
        try {
            OutputStream stream = Files.newOutputStream(Paths.get(fileName));
            Encoder encoder =  EncoderFactory.get().jsonEncoder(schema, stream);
            specificDatumWriter.write(record, encoder);
            encoder.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
