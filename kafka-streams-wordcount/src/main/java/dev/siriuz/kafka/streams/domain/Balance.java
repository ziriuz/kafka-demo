package dev.siriuz.kafka.streams.domain;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class Balance {
    private long amount;
    private LocalDateTime lastUpdate;

    public Balance(long amount, LocalDateTime lastUpdate) {
        this.amount = amount;
        this.lastUpdate = lastUpdate;
    }

    public void add(long amount){
        this.amount += amount;
    }

    public void modifyLastUpdate(LocalDateTime newTime){
        if (newTime.compareTo(lastUpdate) > 0){
            lastUpdate = newTime;
        }
    }

    @Override
    public String toString() {
        return "{\"balance\":"+ amount +", \"lastUpdate\":\"" + lastUpdate + "\"}";
    }
}
