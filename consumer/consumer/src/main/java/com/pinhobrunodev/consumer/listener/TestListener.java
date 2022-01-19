package com.pinhobrunodev.consumer.listener;

import com.pinhobrunodev.consumer.model.Person;
import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.adapter.ConsumerRecordMetadata;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.TimeZone;

@Log4j2
@Component
public class TestListener {

    @KafkaListener(topics = "topic-1", groupId = "group-1")
    public void listen(String message) {
        log.info(message);
    }


    // containerFactory -> Especificando como vai tratas as mensagens vindo do topico (deserialization)
    @KafkaListener(topics = "person-topic", groupId = "group-1",containerFactory = "personConcurrentKafkaListenerContainerFactory")
    public void listenPerson(Person person) {
        log.info("Pessoa: {} ",person);
    }
}
