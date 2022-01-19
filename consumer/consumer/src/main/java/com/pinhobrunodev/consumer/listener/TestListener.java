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

        022-01-19 18:30:39.479  INFO 6524 --- [ntainer#2-0-C-1] c.p.consumer.listener.TestListener       : Partition 1-9: 8 Message: Teste
        2022-01-19 18:30:44.122  INFO 6524 --- [ntainer#3-0-C-1] c.p.consumer.listener.TestListener       : Partition 0: 0 Message: Teste
        2022-01-19 18:30:44.922  INFO 6524 --- [ntainer#2-0-C-1] c.p.consumer.listener.TestListener       : Partition 1-9: 3 Message: Teste
        2022-01-19 18:30:45.918  INFO 6524 --- [ntainer#2-0-C-1] c.p.consumer.listener.TestListener       : Partition 1-9: 4 Message: Teste
        2022-01-19 18:30:46.054  INFO 6524 --- [ntainer#2-0-C-1] c.p.consumer.listener.TestListener       : Partition 1-9: 9 Message: Teste
        2022-01-19 18:30:46.174  INFO 6524 --- [ntainer#2-0-C-1] c.p.consumer.listener.TestListener       : Partition 1-9: 3 Message: Teste
        2022-01-19 18:30:46.286  INFO 6524 --- [ntainer#2-0-C-1] c.p.consumer.listener.TestListener       : Partition 1-9: 7 Message: Teste
        2022-01-19 18:30:46.482  INFO 6524 --- [ntainer#2-0-C-1] c.p.consumer.listener.TestListener       : Partition 1-9: 8 Message: Teste
        2022-01-19 18:30:46.593  INFO 6524 --- [ntainer#2-0-C-1] c.p.consumer.listener.TestListener       : Partition 1-9: 4 Message: Teste
        2022-01-19 18:30:46.702  INFO 6524 --- [ntainer#2-0-C-1] c.p.consumer.listener.TestListener       : Partition 1-9: 2 Message: Teste
        2022-01-19 18:32:03.250  INFO 6524 --- [ntainer#3-0-C-1] c.p.consumer.listener.TestListener       : Partition 0: 0 Message: Teste
        2022-01-19 18:32:03.403  INFO 6524 --- [ntainer#2-0-C-1] c.p.consumer.listener.TestListener       : Partition 1-9: 5 Message: Teste
        2022-01-19 18:32:03.523  INFO 6524 --- [ntainer#2-0-C-1] c.p.consumer.listener.TestListener       : Partition 1-9: 9 Message: Teste
        2022-01-19 18:32:03.626  INFO 6524 --- [ntainer#2-0-C-1] c.p.consumer.listener.TestListener       : Partition 1-9: 6 Message: Teste
        2022-01-19 18:32:03.841  INFO 6524 --- [ntainer#2-0-C-1] c.p.consumer.listener.TestListener       : Partition 1-9: 4 Message: Teste
        2022-01-19 18:32:03.963  INFO 6524 --- [ntainer#2-0-C-1] c.p.consumer.listener.TestListener       : Partition 1-9: 3 Message: Teste
        2022-01-19 18:32:04.042  INFO 6524 --- [ntainer#2-0-C-1] c.p.consumer.listener.TestListener       : Partition 1-9: 9 Message: Teste
        2022-01-19 18:32:04.142  INFO 6524 --- [ntainer#2-0-C-1] c.p.consumer.listener.TestListener       : Partition 1-9: 3 Message: Teste
        2022-01-19 18:32:04.251  INFO 6524 --- [ntainer#3-0-C-1] c.p.consumer.listener.TestListener       : Partition 0: 0 Message: Teste
        2022-01-19 18:32:04.347  INFO 6524 --- [ntainer#2-0-C-1] c.p.consumer.listener.TestListener       : Partition 1-9: 7 Message: Teste
        2022-01-19 18:32:04.444  INFO 6524 --- [ntainer#2-0-C-1] c.p.consumer.listener.TestListener       : Partition 1-9: 6 Message: Teste
