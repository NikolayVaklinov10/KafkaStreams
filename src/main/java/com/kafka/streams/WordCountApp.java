package com.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class WordCountApp {

    public Topology createTopology(){


        // StreamBuilder is a way to write builder application
        StreamsBuilder builder = new StreamsBuilder();
        // 1 - stream from Kafka
        KStream<String, String> wordCountInput= builder.stream("word-count-input");

        // 2 - map values to lowercase

        KTable<String, Long> wordCounts= wordCountInput.mapValues(value -> value.toLowerCase())
                // can be alternatively written as:
                // .mapValues(String::toLowerCase)
                // 3 - flatmap values split by space
                .flatMapValues(lowercasedTextLine -> Arrays.asList(lowercasedTextLine.split(" ")))
                // 4 - select key to apply a key (discarding the old key)
                .selectKey((ignoreKey, word) ->  word)
                // 5 - group by key before aggregation
                .groupByKey()
                // 6 - count occurrences
                .count(Named.as("Counts"));

        // 7 - to in order to write the results back to kafka
        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }


    public static void main(String[] args) throws InterruptedException {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,"word-count-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        WordCountApp wordCountApp = new WordCountApp();

        // to bring all together
        KafkaStreams streams = new KafkaStreams(wordCountApp.createTopology(), config);
        streams.start();
        // printing the Topology
        System.out.println(streams.toString());

    // in this example we just let it run for some time and stop since the input data is finite.
        Thread.sleep(5000L);

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
