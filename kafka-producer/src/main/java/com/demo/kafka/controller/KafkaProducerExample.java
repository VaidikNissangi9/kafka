package com.demo.kafka.controller;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.Properties;

@RestController
@RequestMapping("/kafkaDemo")
public class KafkaProducerExample {

    @Value("${kafka.bootstrap.servers}")
    private String kafkaBootstrapServers;

    @GetMapping("/publish")
    public void produce() {

        String topic = "demo";

        Properties props = new Properties();

        String serializer = StringSerializer.class.getName();

        props.put("bootstrap.servers", kafkaBootstrapServers);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "earliest");
        props.put("session.timeout.ms", "30000");
        props.put("key.serializer", serializer);
        props.put("value.serializer", serializer);
        Producer<String, String> producer = new KafkaProducer<>(props);
        Date d = new Date();
        producer.send(new ProducerRecord<>(topic, d.toString()));
    }
}
