package com.home.kafka.rt.kafkastreams.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;

import java.sql.Timestamp;

@Data
public class Transaction {
    private String transactionId;
    private String accountId;
    private Double amount;
    private String location;
    private Timestamp timestamp;

    private static ObjectMapper objectMapper = new ObjectMapper();

    public static Transaction fromJson(String transactionJson) {
        try {
            return objectMapper.readValue(transactionJson, Transaction.class);
        } catch (JsonProcessingException e) {
            return null;
        }
    }
}
