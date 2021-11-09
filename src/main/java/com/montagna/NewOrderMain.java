package com.montagna;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // Criar um Produtor do Kafka
        var kafkaProducer = new KafkaProducer<String, String>(properties());

        for (var i = 0; i <= 100; i++) {

            // Valores que serão atribuídos a key e ao value
            var key = UUID.randomUUID().toString();
            var value = String.format("ValorKafka, %a, %a", Math.random(), Math.random());
            var emailValue = "EMAIL, Assunto, Enviado";

            // Cria as mensagens que serão enviadas ao topic
            var record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", key, value);

            var emailRecord = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL", key, emailValue);


            // Trata o retorno da mensagem
            Callback callback = (data, ex) -> {
                if (ex != null) {
                    ex.printStackTrace();
                    return;
                }
                System.out.println("Sucesso Enviando " + data.topic() + ":::partition " + data.partition() + "/ offset" + "/" + data.timestamp());
            };

            // Enviar para o tópico do Kafka
            kafkaProducer.send(record, callback).get();
            kafkaProducer.send(emailRecord, callback).get();

            //        kafkaProducer.close();
            //        kafkaProducer.flush();

        }


    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "PLAINTEXT://henrique-virtualbox:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }

}
