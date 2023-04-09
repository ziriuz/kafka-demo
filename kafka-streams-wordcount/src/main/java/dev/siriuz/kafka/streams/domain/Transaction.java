package dev.siriuz.kafka.streams.domain;

import com.github.javafaker.Faker;
import dev.siriuz.kafka.streams.utils.Utils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.time.LocalDateTime;
import java.util.Date;

@Data
@NoArgsConstructor
@ToString
@AllArgsConstructor
public class Transaction {
    private String name;
    private int amount;
    private LocalDateTime timestamp;
    public static Transaction createFakeTransaction(){
        return new Transaction(
                Utils.randomUsername(),
                (int) (2000 * (Math.random() - 0.5)),
                LocalDateTime.now()
        );
    }
}
