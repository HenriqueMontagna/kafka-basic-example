package com.montagna;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

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


