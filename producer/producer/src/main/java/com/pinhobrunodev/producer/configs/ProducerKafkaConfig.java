package com.pinhobrunodev.producer.configs;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

@Configuration
public class ProducerKafkaConfig {

    @Autowired
    private KafkaProperties kafkaProperties;

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        var configs = new HashMap<String, Object>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(configs);
    }

    // Trafegar objetos
//    @Bean
    public ProducerFactory jsonProducerFactory() {
        var configs = new HashMap<String, Object>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(configs, new StringSerializer(), new JsonSerializer<>());
    }

    // Garante dinamicidade
    // Somente 1 KafkaTemplate para tudo
    @Bean
    public RoutingKafkaTemplate routingKafkaTemplate(GenericApplicationContext context,
                                                     ProducerFactory producerFactory){
        var jsonProducerFactory = jsonProducerFactory();
        context.registerBean(DefaultKafkaProducerFactory.class,"jsonPF",jsonProducerFactory());
        // Pattern -> Qual ProducerFactory vai utilizar = nome dos topicos
        // ProducerFactory -> De Qualquer coisa (Object pode ser tanto String como Person)
        Map<Pattern,ProducerFactory<Object,Object>> map = new LinkedHashMap<>();
        map.put(Pattern.compile("topic-.*"),producerFactory); // Todos que usarem nesse formato vai receber String (topic-1)
        map.put(Pattern.compile(".*-topic"),jsonProducerFactory); // Todos que usarem nesse formato vai receber Object (person-topic,city-topic)
        return new RoutingKafkaTemplate(map);
    }


    // Permitindo criar topicos a partir da aplicação produtora
    @Bean
    public KafkaAdmin kafkaAdmin() {
        var configs = new HashMap<String, Object>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        return new KafkaAdmin(configs);
    }


    //2.7 -> Criar varios topicos
    @Bean
    public KafkaAdmin.NewTopics topics() {
        return new KafkaAdmin.NewTopics(
                TopicBuilder.name("topic-1").partitions(2).replicas(1).build(),
//              TopicBuilder.name("my-topic").partitions(10).build(),
                TopicBuilder.name("person-topic").partitions(2).build(),
                TopicBuilder.name("city-topic").partitions(2).build()
        );
    }

}
