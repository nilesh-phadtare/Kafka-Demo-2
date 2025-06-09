package org.nilesh.kafka;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class ConsumerMain {
    //private static Logger logger = LoggerFactory.getLogger(ProducerMain.class);

    public static void main(String[] args) {
        log.info("Kafka Consumer started.");
        // Add Kafka Consumer logic here
        String groupId = "my-group";
        // Producer Properties
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        properties.setProperty("key.deserializer", StringSerializer.class.getName());
        properties.setProperty("value.deserializer", StringSerializer.class.getName());

        properties.setProperty("group.id", groupId);

        //Kafka Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //subscribe to topic
        consumer.subscribe(java.util.Collections.singletonList("demo_topic"));

        //poll for new data
        while (true) {
            consumer.poll(java.time.Duration.ofMillis(1000)).forEach(record -> {
                log.info("Key: " + record.key() + ", Value: " + record.value() +
                        ", Partition: " + record.partition() + ", Offset: " + record.offset());
            });
        }


    }
}
