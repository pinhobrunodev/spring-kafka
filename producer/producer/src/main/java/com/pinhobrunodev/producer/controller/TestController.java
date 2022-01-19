package com.pinhobrunodev.producer.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.stream.IntStream;

@RestController
public class TestController {


    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @GetMapping("send")
    public ResponseEntity<?> send() {
        IntStream.range(1, 10)
                .boxed()
                .forEach(n -> {
                    System.out.println(LocalDateTime.now());
                    kafkaTemplate.send("topic-1", "Mensagem: " + n);
                });
        return ResponseEntity.ok().build();
    }

}
