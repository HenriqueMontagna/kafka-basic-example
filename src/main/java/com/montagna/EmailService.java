package com.montagna;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService {

    public static void main(String[] args) {

        var emailService = new EmailService();

        new KafkaService("ECOMMERCE_SEND_EMAIL", emailService::parse, EmailService.class.getSimpleName()).run();

    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("=============================================");
        System.out.println("Send New Email");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        System.out.println("=============================================");
    }

}


