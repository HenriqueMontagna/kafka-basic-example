package com.montagna;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Properties;
import java.util.regex.Pattern;

public class KafkaService {

    private final KafkaConsumer<String, String> kafkaConsumer;
    private final ConsumerFunction parse;
    private final String consumerGroup;

    KafkaService(String topic, ConsumerFunction parse, String consumerGroup) {
        this.consumerGroup = consumerGroup;
        this.parse = parse;
        this.kafkaConsumer = new KafkaConsumer(properties());
        this.kafkaConsumer.subscribe(Pattern.compile(topic));
    }

    void run(){
        while (true) {
            try {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

                for (var record : records) {
                    parse.consume(record);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    private Properties properties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "henrique-virtualbox:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, this.consumerGroup);
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");

        return properties;
    }

}
