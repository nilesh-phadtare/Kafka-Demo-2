package org.nilesh.kafka;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class ProducerMainWithCallback {
    //private static Logger logger = LoggerFactory.getLogger(ProducerMain.class);

    public static void main(String[] args) {
        log.info("Kafka Producer started.");
        // Add Kafka producer logic here
        // Producer Properties
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Partition batch size
        properties.setProperty("batch.size", "400"); // default - 16KB
        // partition class
        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        //Kafka Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        // producer record
        ProducerRecord<String, String> record = new ProducerRecord<>("demo_topic", "first", "Hello Kafka!");

        // new way to send data with a callback
        // new Callback () ->{ onCompletion(recordMetadata, e) {...}
        // }

//        producer.send(record, new Callback() {
//                    @Override
//                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
//                        /// Callback method
//                    }
//                });

                producer.send(record, (recordMetadata, e) -> {  /// Callback method
                    // executes every time a record is successfully sent or an exception is thrown
                    if (e == null) {
                        // the record was successfully sent
                        log.info("Received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        // an error occurred
                        log.error("Error while producing {}", e.getMessage(), e);
                    }
                });



        // flush and close producer
        producer.flush();
        producer.close();

    }
}
