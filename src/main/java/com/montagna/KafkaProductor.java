package com.montagna;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Properties;
import java.util.UUID;

public class KafkaProductor<T> implements Closeable {

    private final KafkaProducer<String, T> producer;
    private final String topic;

    KafkaProductor(String topic) {
        this.topic = topic;
        this.producer = new KafkaProducer<>(properties());
    }

    void run(T message) {

        String key = UUID.randomUUID().toString();

        ProducerRecord<String, T> record = new ProducerRecord<>(topic, key, message);

        try {
            this.producer.send(record, (recordMetadata, exception) -> {
                if (exception != null) {
                    exception.printStackTrace();
                    return;
                }
                System.out.println("Success " + recordMetadata.topic() + ":::partition " + recordMetadata.partition() + "/ offset/" + recordMetadata.timestamp());
            }).get();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "PLAINTEXT://henrique-virtualbox:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());

        return properties;
    }

    @Override
    public void close() {
        producer.close();
    }
}
