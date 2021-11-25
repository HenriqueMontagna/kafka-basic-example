package com.montagna;

import java.math.BigDecimal;
import java.util.Random;
import java.util.UUID;

public class NewOrderMain {

    public static void main(String[] args) {

        try (KafkaProductor newOrderProducer = new KafkaProductor("ECOMMERCE_NEW_ORDER");
             KafkaProductor sendEmailProducer = new KafkaProductor("ECOMMERCE_SEND_EMAIL")) {

            newOrderProducer.run(new Order(
                    UUID.randomUUID().toString(),
                    String.valueOf(Math.random() * 1000 + 1),
                    BigDecimal.valueOf(100 + (new Random().nextDouble() * (500 - 100)))
            ));

            sendEmailProducer.run(new Email("New Message from {EMAIL}", "Body of {EMAIL}"));

        }

    }

}
