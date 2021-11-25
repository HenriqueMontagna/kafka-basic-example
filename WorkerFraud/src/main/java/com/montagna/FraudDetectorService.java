package com.montagna;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class FraudDetectorService {

    public static void main(String[] args) {

        FraudDetectorService fraudDetectorService = new FraudDetectorService();

        try (var kafkaService = new KafkaService("ECOMMERCE_NEW_ORDER", fraudDetectorService::parse, FraudDetectorService.class.getSimpleName(), Order.class.getName()) ) {
            kafkaService.run();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    private void parse(ConsumerRecord<String, Order> record) {
        System.out.println("=============================================");
        System.out.println("Processing new order, checking for fraud");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        System.out.println("=============================================");
    }

}
