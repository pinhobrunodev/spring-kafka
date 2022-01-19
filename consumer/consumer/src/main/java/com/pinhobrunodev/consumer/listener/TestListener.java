package com.pinhobrunodev.consumer.listener;

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

   /* @KafkaListener(topics = "topic-1", groupId = "group-1")
    public void listen(String message,
                       // Como obter Header - Quando uma aplicação consumidora consome uma mensagem de um determinado topico, e com isso conseguimos capturar algumas info
                       @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                       @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        // log.info("Thread: {}",Thread.currentThread().getId());
        log.info("Topic {} Pt {} : ",topic,partition,message);
    }*/

    @KafkaListener(topics = "topic-1", groupId = "group-1")  // metadata : Pega alguns Headers (Info das msg )
    public void listen(String message, ConsumerRecordMetadata metadata ) {
        // log.info("Thread: {}",Thread.currentThread().getId());
        //OffSet -> Quantas vezes ja ouve uma chamada na determinada partição : Topic topic-1 Pt 0  Offset 44 : Mensagem: 9  -> Partition 0 = 44 calls
        log.info("Topic {} Pt {}  Offset {} : {} ",metadata.topic(),metadata.partition(),metadata.offset(),message);
        log.info("Timestamp {}", LocalDateTime.ofInstant(Instant.ofEpochMilli(metadata.timestamp()), TimeZone.getDefault().toZoneId()));
    }
}
