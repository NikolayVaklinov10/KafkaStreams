package com.kafka.bankApp;

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
        System.out.println(value);



    }

}