package com.pinhobrunodev.consumer.listener;

import com.pinhobrunodev.consumer.custom.PersonCustomListener;
import com.pinhobrunodev.consumer.model.City;
import com.pinhobrunodev.consumer.model.Person;
import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Log4j2
@Component
public class TestListener {

    @KafkaListener(topics = "topic-1", groupId = "group-1", containerFactory = "concurrentKafkaListenerContainerFactory")
    public void listen(String message) {
        log.info("Thread: {} Message: {} ", Thread.currentThread().getId(), message); // Cada listener abre uma Thread
    }

    /*@KafkaListener(topics = "my-topic", groupId = "my-group", containerFactory = "concurrentKafkaListenerContainerFactory")
    public void listen2(String message) {
        log.info("Thread: {} Message: {} ", Thread.currentThread().getId(), message); // Cada listener abre uma Thread
    }
*/
  /*  @KafkaListener(topicPartitions = {@TopicPartition(topic = "my-topic", partitions = "0")}, groupId = "my-group")
    public void listen2(String message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        log.info("Partition 0: {} Message: {} ", partition, message);
    }

    @KafkaListener(topicPartitions = {@TopicPartition(topic = "my-topic", partitions = "1-9")}, groupId = "my-group")
    public void listen3(String message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        log.info("Partition 1-9: {} Message: {} ", partition, message);
    }
*/

    @PersonCustomListener(groupId = "group-1")
    public void createPerson(Person person) {
        log.info("Criar Pessoa: {}", person);
    }

    @KafkaListener(topics = "city-topic", groupId = "group-2", containerFactory = "jsonKafkaListenerContainerFactory")
    public void createCity(City city) {
        log.info("Criar cidade: {}", city);
    }

    /*@PersonCustomListener(groupId = "group-2")
    public void history(Person person) {
        log.info("Thread: {}", Thread.currentThread().getId());
        log.info("Histórico: {} ", person);
    }*/
}
