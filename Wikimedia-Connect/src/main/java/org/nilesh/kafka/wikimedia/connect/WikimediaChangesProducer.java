package org.nilesh.kafka.wikimedia.connect;

import com.launchdarkly.eventsource.ConnectStrategy;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Slf4j
public class WikimediaChangesProducer {
    public static void main(String[] args) throws InterruptedException{
        // TIP: Implement the logic to produce changes from Wikimedia
        System.out.println("Wikimedia Changes Producer started...");
        // Kafka producer configuration would go here
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());
        properties.setProperty("group.id", "wikimedia-group");

        // Here you would typically set up a Kafka producer and start sending messages
        // For example:
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        String topic = "wikimedia.recent.changes";
        String wikimediaStreamUrl = "https://stream.wikimedia.org/v2/stream/recentchange";
       log.info("Connecting to Wikimedia stream at: {}", wikimediaStreamUrl);
        //EventHandler would be implemented to fetch changes from Wikimedia
        BackgroundEventHandler eventHandler = new WikimediaEventHandler(producer, topic);
        // Create an BackgroundEventSource to listen to the Wikimedia stream
        BackgroundEventSource eventSource = new BackgroundEventSource.Builder(
                eventHandler,
                new EventSource.Builder(
                        ConnectStrategy.http(URI.create(wikimediaStreamUrl))))
                .build();


        // Start the event source to listen for changes
        eventSource.start();

        //block Code
        TimeUnit.MINUTES.sleep(5);

        // TIP: Don't forget to close the producer when done
        // producer.close();
    }
}
