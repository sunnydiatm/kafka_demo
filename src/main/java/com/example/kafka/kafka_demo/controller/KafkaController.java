package com.example.kafka.kafka_demo.controller;

import com.example.kafka.kafka_demo.model.User;
import com.example.kafka.kafka_demo.producer.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(value = "/kafka")
public class KafkaController {

    @Autowired
    private KafkaTemplate<String, User> kafkaTemplate;

    @Autowired
    Publisher publisher;

    private static final String TOPIC = "Kafka_Example";

    @GetMapping("/publish/{name}")
    public String post(@PathVariable("name") final String name) {

        kafkaTemplate.send(TOPIC, new User(name, "Technology"));

        return "Published successfully";
    }

    private static final String TOPIC_JSON = "Kafka_Example_json";

    @GetMapping("/publishJson/{name}")
    public String postJson(@PathVariable("name") final String name) {

        //kafkaTemplate.send(TOPIC_JSON, new User(name, "JSON Technology"));
        publisher.publishEvent(name, "JSON Technology");

        return "Published successfully as JSON message";
    }

}
