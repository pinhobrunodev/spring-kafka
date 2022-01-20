package com.pinhobrunodev.consumer.configs;

import com.pinhobrunodev.consumer.model.Person;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;

@Log4j2
@EnableKafka // So fica na consumidora
@Configuration
public class ConsumerKafkaConfig {

    @Autowired
    private KafkaProperties kafkaProperties;

    // String
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        var configs = new HashMap<String, Object>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(configs);
    }


    // Definindo o que a aplicação vai consumir
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> concurrentKafkaListenerContainerFactory() {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(2); // Setando automaticamente a abertura de 2 Threads para cada KafkaListener
        return factory;
    }


    @Bean
    public ConsumerFactory jsonConsumerFactory() {
        var configs = new HashMap<String, Object>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(configs);
    }


    // Definindo o que a aplicação vai consumir
    @Bean
    public ConcurrentKafkaListenerContainerFactory jsonKafkaListenerContainerFactory() {
        var factory = new ConcurrentKafkaListenerContainerFactory();
        factory.setConsumerFactory(jsonConsumerFactory());
        factory.setMessageConverter(new JsonMessageConverter()); // Deixa a responsabilidade para o Listening informar qual tipo de deserialização
        // Ex : Quando chegar no Listening de Pessoa ele vai buscar desererializar pra esse tipo de classe.
        return factory;
    }


    @Bean
    public ConsumerFactory<String, Person> personConsumerFactory() {
        var configs = new HashMap<String, Object>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        var jsonDeserializer = new JsonDeserializer<>(Person.class)// Tipo que foi deserializado, por isso que precisa 1 pra cada.
                .trustedPackages("*") // Definido qual pacote de objetos que estao vindo que sao confiavies ( pacote person da produtora, por exemplo)
                // * -> todos pacotes
                .forKeys();   // Buscar somente a da Consumidora (Person)
        return new DefaultKafkaConsumerFactory<>(configs, new StringDeserializer(), jsonDeserializer);
    }

    // Definindo o que a aplicação vai consumir
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Person> personConcurrentKafkaListenerContainerFactory() {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, Person>();
        factory.setConsumerFactory(personConsumerFactory());
        factory.setRecordInterceptor(adultInterceptor()); // se tiver mais de 18 anos vai passar a msg e vai para os listenings que tem esse ContainerFactory
        return factory;
    }

    // Exemplo minha consumidora vai consumir algum topico e quer filtrar por um estado das informações.

    // Se eu gerar um person que idade >= 18 vai pro Listening que esta utilizando a minha personConcurrentKafkaListenerContainerFactory
    // Se for < 18 vai printar so o record.
    private RecordInterceptor<String, Person> adultInterceptor() {
        return record -> {
            log.info("Record: {}",record);
            var person = record.value();
            return person.getAge() >= 18 ? record : null;
        };
    }


}
