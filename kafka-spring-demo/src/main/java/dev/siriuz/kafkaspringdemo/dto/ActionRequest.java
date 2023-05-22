package dev.siriuz.kafkaspringdemo.dto;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class ActionRequest {
    private String requestId;
    private String payload;
}
