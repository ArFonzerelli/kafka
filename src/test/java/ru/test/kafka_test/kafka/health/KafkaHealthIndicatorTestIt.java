package ru.test.kafka_test.kafka.health;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.kafka.core.KafkaAdmin;
import ru.test.kafka_test.kafka.CommonValues;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class KafkaHealthIndicatorTestIt {

    private KafkaAdmin kafkaAdmin;

    @BeforeEach
    public void init() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, CommonValues.KAFKA_SERVER);
        this.kafkaAdmin = new KafkaAdmin(configs);
    }

    private void initTopics(AdminClient adminClient, Map<String, Integer> topics) {
        List<NewTopic> newTopics = new ArrayList<>();
        for (Map.Entry<String, Integer> topic : topics.entrySet()) {
            newTopics.add(new NewTopic(topic.getKey(), 1, topic.getValue().shortValue()));
        }

        adminClient.createTopics(newTopics);
    }

    @Test
    public void healthUp() {
        Map<String, Integer> topics = new HashMap<>();
        topics.put("topic1", 1);
        topics.put("topic2", 2);
        topics.put("topic3", 3);

        AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfig());
        initTopics(adminClient, topics);

        KafkaHealthIndicator kafkaHealthIndicator = new KafkaHealthIndicator(adminClient, topics.keySet());
        Health health = kafkaHealthIndicator.health();
        adminClient.close();

        assertEquals(Status.UP, health.getStatus());
        assertEquals(3, Integer.parseInt(String.valueOf(health.getDetails().get("replicationFactor"))));
    }

}
