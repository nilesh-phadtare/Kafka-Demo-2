package org.nilesh.kafka;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

@Slf4j
public class ProducerMain {
    //private static Logger logger = LoggerFactory.getLogger(ProducerMain.class);

    public static void main(String[] args) {
        log.info("Kafka Producer started.");
        // Add Kafka producer logic here
        // Producer Properties
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //Kafka Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        // producer record
        ProducerRecord<String, String> record = new ProducerRecord<>("demo_topic", "first", "Hello Kafka!");
        // send data
        producer.send(record);

        // flush and close producer
        producer.flush();
        producer.close();

    }
}
