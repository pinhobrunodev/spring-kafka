package com.pinhobrunodev.consumer2.listener;

import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Log4j2
@Component
public class TestListener2 {


    @KafkaListener(topics = "topic-1",groupId = "group-2")
    public void listen(String message){
        //log.info("Thread: {}",Thread.currentThread().getId());
        log.info(message);
    }
}