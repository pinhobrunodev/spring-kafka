package com.pinhobrunodev.producer.configs;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;

@Configuration
public class ProducerKafkaConfig {

    @Autowired
    private KafkaProperties kafkaProperties; // Usado para pegar as configs do Kafka ( Exemplo : endereço )

    // Factory => Serialização das Mensagens e Endereço do Broker
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        var configs = new HashMap<String, Object>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers()); // Setando o valor do endereço do meu broker.
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class); // Setando como vai ser feito a serialização da Chave
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);// Setando como vai ser feito a serialização dos Valores do payload
        return new DefaultKafkaProducerFactory<>(configs);
    }

    // Template => Usado para envio das mensagens
    // <K,V> -> Chave String e valor String
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    // Permitindo criar topicos a partir da aplicação produtora
    @Bean
    public KafkaAdmin kafkaAdmin() {
        var configs = new HashMap<String, Object>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        return new KafkaAdmin(configs);
    }

    // Tópico1 Criado.
    @Bean
    public NewTopic topic1() { // Qnd mais de 1 consumer se conecta  a mais de 1 topico no msm grupo, as partições são divididas
                               // Se tiver 11 consumidores não vai ter topico suficiente
        return new NewTopic("topic-1", 10, Short.valueOf("1"));
        // 2.6
//        return TopicBuilder.name("topic-1").build();
    }

    //2.7 -> Criar varios topicos
    @Bean
    public KafkaAdmin.NewTopics topics() {
        return new KafkaAdmin.NewTopics(
                TopicBuilder.name("topic-1").build(),
                TopicBuilder.name("topic-1").build(),
                TopicBuilder.name("topic-1").build(),
                TopicBuilder.name("topic-1").build()
        );
    }

}
