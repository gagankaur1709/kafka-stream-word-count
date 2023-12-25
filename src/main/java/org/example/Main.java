package org.example;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

public class Main {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count"); // imp as consumerGroupId = applicationID
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092"); // connect to kafka cluster
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");// read from earliest data available
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass()); // serialization and deserialization
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        // We take a stream from kafka
        KStream<String, String> wordCountInput = builder.stream("word-count-input");
        // map values to lower case
        KTable<String, Long> wordCounts = wordCountInput.mapValues(value -> value.toLowerCase())
        //Flat map values split by space
                .flatMapValues(value -> Arrays.asList(value.split(" ")))
        // select key to apply a key (key=value)
                .selectKey(((key, value) -> value))
        // group by aggregation
                .groupByKey()
        // count occurrence of each group
                .count(Named.as("counts"));
        // write results back to kafka
        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

        try {
            KafkaStreams streams = new KafkaStreams(builder.build(), properties);
            streams.start();
            // print the topology
            System.out.println(streams.toString());
            // shutdown hook to correctly close the streams application
            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        } catch (Exception e) {
            System.out.println("Exception occurred while counting");
        }
    }
}