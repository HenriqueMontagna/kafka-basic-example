package com.montagna;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class LogService {

    public static void main(String[] args) {

        LogService logService = new LogService();

        KafkaService consumer = new KafkaService("ECOMMERCE.*", logService::parse, LogService.class.getSimpleName());

        consumer.run();

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
