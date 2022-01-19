package com.pinhobrunodev.consumer.listener;

import com.pinhobrunodev.consumer.custom.PersonCustomListener;
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

    //concurrency = "2" -> Quantidade de Threads que quero q abra -> DEPENDE MUITO DA QUANTIDADE DE PARTIÇÕES
    // se concurrency > partitions = BAD
    // Muito usado para escalonar o Listening
    @KafkaListener(topics = "topic-1", groupId = "group-1",concurrency = "2")
    public void listen(String message) {
        log.info("Thread: {} Message: {} ", Thread.currentThread().getId(),message); // Cada listener abre uma Thread
    }

                       // Definindo que esse Listen vai atuar somente na partição 0                                                           //0-5,9
    @KafkaListener(topicPartitions = {@TopicPartition(topic = "my-topic",partitions = "0")},groupId = "my-group")
    public void listen2(String message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        log.info("Partition 0: {} Message: {} ", partition,message);
    }
             // Definindo que esse Listen vai atuar nas partições de 1-9
    @KafkaListener(topicPartitions = {@TopicPartition(topic = "my-topic",partitions = "1-9")},groupId = "my-group")
    public void listen3(String message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        log.info("Partition 1-9: {} Message: {} ", partition,message);
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

