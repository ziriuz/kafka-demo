package dev.siriuz.kafka.streams.utils;

import com.github.javafaker.Faker;
import com.google.gson.*;
import dev.siriuz.kafka.streams.domain.Balance;
import dev.siriuz.kafka.streams.domain.Transaction;

import java.lang.reflect.Type;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

public class Utils {

    private static final List<String> usernames;
    private static final Gson gson;
    static {
        usernames = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            usernames.add(Faker.instance().name().firstName());
        }

        gson = new GsonBuilder()
                .registerTypeAdapter(LocalDateTime.class,
                    (JsonDeserializer<LocalDateTime>) (json, type, jsonDeserializationContext) -> {
                        return LocalDateTime.parse(json.getAsJsonPrimitive().getAsString());
                    }
                    )
                .registerTypeAdapter(LocalDateTime.class,
                    (JsonSerializer<LocalDateTime>) (localDateTime, type, jsonSerializationContext) -> jsonSerializationContext.serialize(localDateTime.toString())
                )
                .create();
    }

    public static String randomUsername(){
        return usernames.get((int) (19 * Math.random()));
    }

    public static String transactionToJson(Transaction transaction){
        return gson.toJson(transaction);
    }

    public static Transaction jsonToTransaction(String value) {
        return gson.fromJson(value, Transaction.class);
    }

    public static String balanceToJson(Balance balance){
        System.out.println("===== " + balance);
        return gson.toJson(balance);
    }
    public static Balance jsonToBalance(String value) {
        System.out.println("----- "+ value);
        return gson.fromJson(value, Balance.class);
    }
}
