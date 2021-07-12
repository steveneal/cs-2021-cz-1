package com.cs.rfq.decorator;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducer {

    private final static String TOPIC = "test1";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    private static Producer<Long, String> kafkaProducer = null;

    private KafkaProducer() {}

    private static Producer<Long, String> createProducer() {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new org.apache.kafka.clients.producer.KafkaProducer<>(props);
    }


    public static void sendMessage(String metadata) {
        if(kafkaProducer == null)
            kafkaProducer = createProducer();
        kafkaProducer.send(new ProducerRecord<>(TOPIC, metadata));
    }
}