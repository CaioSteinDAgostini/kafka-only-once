/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 */
package com.learning.kafka.only.once;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import static org.apache.kafka.common.serialization.Serdes.Float;
import static org.apache.kafka.common.serialization.Serdes.String;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

/**
 *
 * @author caio
 */
public class KafkaStreamTransactionReturnString {
    //based on https://www.baeldung.com/java-kafka-streams

    public static void main(String[] args) throws InterruptedException {

        String boostrapServers = "127.0.0.1:9092";
        String appName = "transactionString";

        Path stateDirectory;
        String topic = "transaction";

        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        try {
            stateDirectory = Files.createTempDirectory("kafka-streams");
            streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDirectory.toAbsolutePath().toString());
        } catch (final IOException e) {
            e.printStackTrace();
        }

        Topology topology = createTopology(topic);
        KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);
        streams.start();

        Thread.sleep(20000);
        streams.close();

    }

    public static Topology createTopology(java.lang.String topic) {
        // when
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> transactionsLogs = builder.stream(topic);

        KStream<String, String> transactions = transactionsLogs.filter((k, v) -> !v.isBlank());
        Aggregator<String, String, String> aggregator = (key, transaction, oldRecord) -> append(oldRecord, transaction);
        KTable<String, String> transactionsTable = transactions.groupByKey().aggregate(
                () -> "{\"amount\":0}",
                aggregator,
                Materialized.with(String(), String())
        //                        "   "
        );

//        transactionsTable.toStream()
//                .foreach((name, transaction) -> System.out.println("client: " + name + " -> " + transaction));

        transactionsTable.toStream().to("balance");
        final Topology topology = builder.build();
        return topology;
    }

    public static Transaction convert(String string) {
        ObjectMapper om = new ObjectMapper();
        try {
            return om.readValue(string, Transaction.class);
        } catch (JsonProcessingException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    public static String append(String old, String newTransaction) {
        try {
            ObjectMapper om = new ObjectMapper();
            Transaction oldT = om.readValue(old, Transaction.class);
            Transaction newT = om.readValue(newTransaction, Transaction.class);
            return oldT.merge(newT).toString();
        } catch (JsonProcessingException ex) {
            return null;
        }

    }
}
