package com.montagna;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.util.Map;

public class LogService {

    public static void main(String[] args) {

        LogService logService = new LogService();

        try (KafkaService consumer = new KafkaService(
                "ECOMMERCE.*",
                logService::parse,
                LogService.class.getSimpleName(),
                String.class.getName(),
                Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()))) {
            consumer.run();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void parse(ConsumerRecord<String, String> consumerRecord) {
        System.out.println("============ LOG SERVICE ============");
        System.out.println(consumerRecord.topic());
        System.out.println(consumerRecord.value());
        System.out.println(consumerRecord.key());
        System.out.println(consumerRecord.partition());
        System.out.println("=====================================");
    }
}
