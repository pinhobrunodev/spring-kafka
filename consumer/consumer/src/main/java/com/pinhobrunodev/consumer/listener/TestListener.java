package com.pinhobrunodev.consumer.listener;

import com.pinhobrunodev.consumer.custom.PersonCustomListener;
import com.pinhobrunodev.consumer.model.Person;
import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Log4j2
@Component
public class TestListener {

    @KafkaListener(topics = "topic-1", groupId = "group-1")
    public void listen(String message) {
        log.info(message);
    }


    // containerFactory -> Especificando como vai tratas as mensagens vindo do topico (deserialization)
    // @KafkaListener(topics = "person-topic", groupId = "group-1",containerFactory = "personConcurrentKafkaListenerContainerFactory")
    @PersonCustomListener(groupId = "group-1")
    public void create(Person person) {
        log.info("Thread: {}", Thread.currentThread().getId()); // Cada listener abre uma Thread
        log.info("Criar: {} ", person);
    }

    // Ambos vao estar escutando o mesmo topico e realizando a mesma deserializaçao de Pessoa porem com group-id diferente

    // @KafkaListener(topics = "person-topic", groupId = "group-2",containerFactory = "personConcurrentKafkaListenerContainerFactory")
    @PersonCustomListener(groupId = "group-2")
    public void history(Person person) {
        log.info("Thread: {}", Thread.currentThread().getId());
        log.info("Histórico: {} ", person);
    }
}
