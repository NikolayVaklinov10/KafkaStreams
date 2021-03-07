package com.kafka.bankApp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import static org.junit.jupiter.api.Assertions.*;

public class BankTransactionsProducerTest {

    @Test
    public void newRandomTransactionTest(){
        ProducerRecord<String, String> record = BankTransactionsProducer.newRandomTransaction("john");
        String key = record.key();
        String value = record.value();

        assertEquals(key, "john");

        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode node = mapper.readTree(value);
            assertEquals(node.get("name").asText(), "john");
            assertTrue( node.get("amount").asInt() < 100);
//            assertEquals(node.get("time").asText(), "john");
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        System.out.println(value);



    }

}