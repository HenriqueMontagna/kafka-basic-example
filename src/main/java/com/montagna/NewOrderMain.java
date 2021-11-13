package com.montagna;

public class NewOrderMain {

    public static void main(String[] args) {

        try (KafkaProductor newOrderProducer = new KafkaProductor("ECOMMERCE_NEW_ORDER");
             KafkaProductor sendEmailProducer = new KafkaProductor("ECOMMERCE_SEND_EMAIL")) {

            newOrderProducer.run("Messsage from Order {NEW ORDER}");
            sendEmailProducer.run("Message from Email {NEW EMAIL}");

        }

    }

}
