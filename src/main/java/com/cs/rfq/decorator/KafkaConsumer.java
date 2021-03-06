package com.cs.rfq.decorator;

import com.google.gson.JsonSyntaxException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class KafkaConsumer {

    private final static String TOPIC = "test";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    private static Consumer<Long, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Create the kafkaConsumer using props.
        final Consumer<Long, String> kafkaConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer(props);

        // Subscribe to the topic.
        kafkaConsumer.subscribe(Collections.singletonList(TOPIC));
        return kafkaConsumer;
    }

    static void runConsumer(RfqProcessor rfqProcessor) {
        final Consumer<Long, String> consumer = createConsumer();

        while (true) {
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);

            consumerRecords.forEach(record -> {
                try {
                    Rfq rfq = Rfq.fromJson(record.value());
                    rfqProcessor.processRfq(rfq);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

            consumer.commitAsync();
        }
    }

}