package com.pinhobrunodev.producer.controller;

import com.pinhobrunodev.producer.model.Person;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.Serializable;
import java.util.Random;

@RestController
public class TestController {


    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    private KafkaTemplate<String, Serializable> jsonKafkaTemplate;
    @GetMapping("send-person")
    public void sendPerson() {
        jsonKafkaTemplate.send("person-topic", new Person("Bruno", new Random().nextInt(50)));
    }
    @GetMapping("send")
    public void send() {
        kafkaTemplate.send("topic-1", "Olá Mundo!");
    }


}
