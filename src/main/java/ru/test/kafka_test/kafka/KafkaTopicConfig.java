package ru.test.kafka_test.kafka;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaTopicConfig {

    public static final String TOPIC_NAME = "fnztest";

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, CommonValues.KAFKA_SERVER);

        return new KafkaAdmin(configs);
    }

    @Bean
    public NewTopic topic1() {
        return new NewTopic(TOPIC_NAME, 1, (short) 2);
    }

    @Bean
    public NewTopic topic2() {
        return new NewTopic("topicrepfact3", 1, (short) 3);
    }

    @Bean
    public NewTopic topic3() {
        return new NewTopic("topicrepfact1", 1, (short) 1);
    }

    @Bean
    public NewTopic topic4() {
        return new NewTopic("topicrepfact2", 1, (short) 2);
    }

}
