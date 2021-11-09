package com.montagna;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Properties;
import java.util.regex.Pattern;

public class LogService {

    public static void main(String[] args) {

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties());
        consumer.subscribe(Pattern.compile("ECOMMERCE.*"));

        while (true) {

            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                if (!records.isEmpty()) {
                    System.out.println("Encontramos mensagens para consumir");
                    records.forEach(consumerRecord -> {
                        System.out.println("============ LOG SERVICE ============");
                        System.out.println(consumerRecord.topic());
                        System.out.println(consumerRecord.value());
                        System.out.println(consumerRecord.key());
                        System.out.println(consumerRecord.partition());
                        System.out.println("=====================================");
                    });

                }

            } catch (Exception ex) {
                ex.printStackTrace();
            }

        }

    }

    private static Properties properties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "henrique-virtualbox:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, LogService.class.getSimpleName());

        return properties;
    }

}
