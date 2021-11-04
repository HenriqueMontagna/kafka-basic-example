package com.montagna;

import org.apache.kafka.clients.producer.Callback;
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

        // Valores que serão atribuídos a key e ao value
        var value = "ValorKafka1,456456,464567";
        var emailValue = "EMAIL, Assunto, Enviado";

        // Cria as mensagens que serão enviadas ao topic
        var record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", value, value);

        var emailRecord = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL", emailValue, emailValue);


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

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "PLAINTEXT://henrique-virtualbox:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }

}
