package org.nilesh.kafka;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class ConsumerShutdownMain {
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
        //rebalance strategy -- default is RangeAssignor,
        // other options include RoundRobinAssignor, StickyAssignor, CooperativeStickyAssignor
        properties.setProperty("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RoundRobinAssignor");

        //Kafka Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        // Set the consumer to be non-blocking
        final Thread mainthread = Thread.currentThread();

        // Set a shutdown hook with wake-up logic
        // This will ensure that the consumer is closed gracefully when the application is terminated
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown hook triggered. Closing consumer.");
            consumer.wakeup(); // This will throw an exception to break the poll loop
            try {
                mainthread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));


        try {
            //subscribe to topic
            consumer.subscribe(java.util.Collections.singletonList("demo_topic"));

            //poll for new data
            while (true) {
                consumer.poll(java.time.Duration.ofMillis(1000)).forEach(record -> {
                    log.info("Key: " + record.key() + ", Value: " + record.value() +
                            ", Partition: " + record.partition() + ", Offset: " + record.offset());
                });
            }
        }catch (WakeupException we){
            // This exception is expected when wakeup() is called
            log.info("Consumer wakeup exception caught. Exiting poll loop.");
        } catch (Exception e) {
            log.error("Error while consuming messages: ", e);
        } finally {
            consumer.close(); // Ensure the consumer is closed properly
            log.info("Consumer closed.");
        }


    }
}
