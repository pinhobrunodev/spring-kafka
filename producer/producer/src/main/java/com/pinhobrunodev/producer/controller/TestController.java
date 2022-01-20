package com.pinhobrunodev.producer.controller;

import com.pinhobrunodev.producer.model.City;
import com.pinhobrunodev.producer.model.Person;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.RoutingKafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.Serializable;
import java.util.Random;
import java.util.stream.IntStream;

@RestController
public class TestController {

//
//    @Autowired
//    private KafkaTemplate<String, String> kafkaTemplate;
//    @Autowired
//    private KafkaTemplate<String, Serializable> jsonKafkaTemplate;

    @Autowired
    private RoutingKafkaTemplate kafkaTemplate;


    @GetMapping("send-person")
    public void sendPerson() {
        kafkaTemplate.send("person-topic", new Person("Bruno", new Random().nextInt(50)));
    }

    @GetMapping("send-city")
    public void sendCity() {
        kafkaTemplate.send("city-topic", new City("Salvador", "BA"));
    }

    @GetMapping("send")
    public void send() {
        // IntStream.range(0, 51).boxed().forEach(n -> kafkaTemplate.send("topic-1", "Número: " + n));
        kafkaTemplate.send("topic-1", "Teste do topic-1");
    }


}
