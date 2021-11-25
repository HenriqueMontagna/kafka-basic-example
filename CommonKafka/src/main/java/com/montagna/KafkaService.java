package com.montagna;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class KafkaService<T> implements Closeable {

    private final KafkaConsumer<String, T> kafkaConsumer;
    private final ConsumerFunction parse;
    private final String consumerGroup;
    private final String type;

    KafkaService(String topic, ConsumerFunction parse, String consumerGroup, String type) {
        this.consumerGroup = consumerGroup;
        this.parse = parse;
        this.type = type;
        this.kafkaConsumer = new KafkaConsumer(getProperties(Map.of()));
        this.kafkaConsumer.subscribe(Pattern.compile(topic));
    }

    KafkaService(String topic, ConsumerFunction parse, String consumerGroup, String type, Map properties) {
        this.consumerGroup = consumerGroup;
        this.parse = parse;
        this.type = type;
        this.kafkaConsumer = new KafkaConsumer(getProperties(properties));
        this.kafkaConsumer.subscribe(Pattern.compile(topic));
    }

    void run(){
        while (true) {
            try {
                ConsumerRecords<String, T> records = kafkaConsumer.poll(Duration.ofMillis(100));

                for (var record : records) {
                    parse.consume(record);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    private Properties getProperties(Map overrideProperties) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "henrique-virtualbox:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, this.consumerGroup);
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        properties.setProperty(GsonDeserializer.TYPE_CONFIG, this.type);
        properties.putAll(overrideProperties);

        return properties;
    }

    @Override
    public void close() throws IOException {
        this.kafkaConsumer.close();
    }
}
