package ru.test.kafka_test.kafka.health;

import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.*;

@Configuration
public class KafkaHealthIndicatorConfig {

    @Bean
    @Scope("prototype")
    public KafkaHealthIndicator kafkaHealthIndicator(KafkaAdmin kafkaAdmin, Set<String> topics) {
        return new KafkaHealthIndicator(kafkaAdmin, topics, 10000);
    }

    @Bean
    public Set<String> topics() {
        return new HashSet<>(Arrays.asList("topicrepfact1", "topicrepfact2", "topicrepfact3"));
    }
}
