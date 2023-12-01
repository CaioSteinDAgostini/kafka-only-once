/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/UnitTests/JUnit5TestClass.java to edit this template
 */
package com.learning.kafka.only.once;

import com.learning.kafka.only.once.KafkaStreamTransactionReturnString;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;

import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 *
 * @author caio
 */
public class KafkaStreamTransactionTest {

    TopologyTestDriver ttd;
    KafkaStreamTransactionReturnString kst;

    public KafkaStreamTransactionTest() {
    }

    @BeforeEach
    public void before() {
        kst = new KafkaStreamTransactionReturnString();
        Topology topology = kst.createTopology("transaction");
        Properties config = new Properties();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        ttd = new TopologyTestDriver(topology, config);
    }

    @AfterEach
    public void after() {
        ttd.close();
    }

    @Test
    public void testTopology() {

        final Instant baseTime = Instant.now();
        final Duration advance = Duration.ofMinutes(5);

        TestInputTopic<String, String> input = ttd.createInputTopic("transaction", new StringSerializer(), new StringSerializer(), baseTime, advance);
        TestOutputTopic<String, String> output = ttd.createOutputTopic("balance", new StringDeserializer(), new StringDeserializer());

        input.pipeInput(new TestRecord<>("dewey", "{\"name\":\"dewey\",\"amount\":100.00,\"timestamp\":\"2023-11-29T23:44:19.991283371Z\"}"));
        input.pipeInput(new TestRecord<>("louie", "{\"name\":\"louie\",\"amount\":838.6442,\"timestamp\":\"2023-11-29T23:44:20.193627924Z\"}"));
        input.pipeInput(new TestRecord<>("huey", "{\"name\":\"huey\",\"amount\":663.76605,\"timestamp\":\"2023-11-29T23:44:20.395565537Z\"}"));
        input.pipeInput(new TestRecord<>("dewey", "{\"name\":\"dewey\",\"amount\":500.00,\"timestamp\":\"2023-11-29T23:44:20.597437156Z\"}"));

        List<String> values = output.readValuesToList();
        String value = values.get(values.size() - 1);
        Assertions.assertTrue(value.equals("{\"name\":\"dewey\",\"amount\":600.0,\"timestamp\":\"2023-11-29T23:44:20.597437156Z\"}"));
    }

}
