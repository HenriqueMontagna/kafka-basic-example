package com.montagna;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // Criar um Produtor do Kafka
        var kafkaProducer = new KafkaProducer<String, String>(properties());
        var value = "ValorKafka1,456456,46456";
        var record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", value, value);

        // Enviar para o tópico do Kafka
        kafkaProducer.send(record,(data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            System.out.println("Sucesso Enviando " + data.topic() + ":::partition " + data.partition() + "/ offset" + "/" + data.timestamp());
        }).get();
//        kafkaProducer.close();
//        kafkaProducer.flush();
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "henrique-virtualbox:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }

}
