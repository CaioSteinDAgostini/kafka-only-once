/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.learning.kafka.only.once;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 *
 * @author caio
 */
public class TransactionsProducer {

    public static void main(String[] args) throws InterruptedException, JsonProcessingException {

        String topic = "transaction";
        String boostrapServers = "127.0.0.1:9092";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "5");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        Producer<String, String> producer = new KafkaProducer<>(properties);

        String huey = "huey";
        String dewey = "dewey";
        String louie = "louie";

        int count = 0;
        while (count < 1000000) {

            producer.send(new ProducerRecord<>(topic, huey, createTransaction(huey)));
            Thread.sleep(200);
            producer.send(new ProducerRecord<>(topic, dewey, createTransaction(dewey)));
            Thread.sleep(200);
            producer.send(new ProducerRecord<>(topic, louie, createTransaction(louie)));
            Thread.sleep(200);

            count++;
        }
        
        producer.close();

    }

    static String createTransaction(String name) throws JsonProcessingException {
        
        Transaction t = new Transaction(name, ThreadLocalRandom.current().nextFloat(0.1f, 1000f),Instant.now().toString());
        ObjectMapper om = new ObjectMapper();
        String transaction = om.writeValueAsString(t);
        System.err.println(transaction);
        return transaction;
    }
}
