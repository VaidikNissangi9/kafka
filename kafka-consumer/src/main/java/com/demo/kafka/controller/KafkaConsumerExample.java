package com.demo.kafka.controller;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.Properties;

@RestController
@RequestMapping("/kafkaDemo")
public class KafkaConsumerExample {

    @GetMapping("/publish")
    public void produce() {

        String topic = "demo";

        Properties props = new Properties();

        String deSerializer = StringDeserializer.class.getName();

        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "consumer");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "earliest");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", deSerializer);
        props.put("value.deserializer", deSerializer);
        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        ConsumerRecords<String, String> records = consumer.poll(1000);
        for (ConsumerRecord<String, String> record : records) {
            System.out.printf("topic: %s partition[%d] offset=%d, key=%s, value=\"%s\"\n",
                    record.topic(), record.partition(),
                    record.offset(), record.key(), record.value());
        }
    }
}
