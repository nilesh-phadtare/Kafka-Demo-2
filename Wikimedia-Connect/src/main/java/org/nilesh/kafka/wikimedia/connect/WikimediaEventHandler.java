package org.nilesh.kafka.wikimedia.connect;

import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
public class WikimediaEventHandler implements BackgroundEventHandler {
    private KafkaProducer<String, String> producer;
    private String topic;

    public WikimediaEventHandler(KafkaProducer<String, String> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    @Override
    public void onOpen() throws Exception {
        // do nothing
    }

    @Override
    public void onClosed() throws Exception {
        // do nothing
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws Exception {
        log.info("Received message: {}", messageEvent.getData());
        producer.send(new ProducerRecord<>(topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String s) throws Exception {
        // do nothing
    }

    @Override
    public void onError(Throwable throwable) {
        log.error("Error in WikimediaEventHandler: ", throwable);
    }
}
