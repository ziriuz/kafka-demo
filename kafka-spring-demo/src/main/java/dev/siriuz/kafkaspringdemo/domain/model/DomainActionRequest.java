package dev.siriuz.kafkaspringdemo.domain.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class DomainActionRequest {

    private String requestId;
    private String payload;


}
