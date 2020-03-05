package ru.test.kafka_test.kafka.health;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class KafkaHealthIndicator implements HealthIndicator {

    private static final Logger logger = LoggerFactory.getLogger(KafkaHealthIndicator.class);

    private long timeOut;

    private AdminClient adminClient;

    private Set<String> topics;

    @Autowired
    public KafkaHealthIndicator(AdminClient adminClient, Set<String> topics) {
        this(adminClient, topics, 10000);
    }

    @Autowired
    public KafkaHealthIndicator(AdminClient adminClient, Set<String> topics, long timeOut) {
        this.adminClient = adminClient;
        this.topics = topics;
        this.timeOut = timeOut;
    }

    @Override
    public Health health() {
        Map<String, String> details = new HashMap<>();
        int nodes = -1, maxReplicationFactor = -1;

        try {
            maxReplicationFactor = getMaxReplicationFactor(topics);
            String replicationFactor = maxReplicationFactor != -1 ? String.valueOf(maxReplicationFactor) : "unknown";
            details.put("replicationFactor", replicationFactor);

            DescribeClusterResult describeClusterResult = adminClient.describeCluster(new DescribeClusterOptions());
            details.put("clusterId", describeClusterResult.clusterId().get(timeOut, TimeUnit.MILLISECONDS));

            List<Node> nodeList = new ArrayList<>(describeClusterResult.nodes().get(timeOut, TimeUnit.MILLISECONDS));
            nodes = nodeList.size();
            details.put("nodes", String.valueOf(nodeList.size()));

            ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(nodeList.get(0).id()));
            DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(Collections.singleton(configResource));
            Map<ConfigResource, Config> configResourceConfigMap = describeConfigsResult.all().get(timeOut, TimeUnit.MILLISECONDS);
            Config config = configResourceConfigMap.get(configResource);

            details.put("brokerId", config.get("broker.id").value());
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            logger.warn(e.toString());
            return Health.status(getStatus(nodes, maxReplicationFactor)).withException(e).withDetails(details).build();
        }

        return Health.status(getStatus(nodes, maxReplicationFactor)).withDetails(details).build();

    }

    private Status getStatus(int nodes, int maxReplicationFactor) {
        return nodes != -1 && maxReplicationFactor != -1 && nodes >= maxReplicationFactor ? Status.UP : Status.DOWN;
    }

    private int getMaxReplicationFactor(Set<String> topics) throws InterruptedException, ExecutionException, TimeoutException {
        Map<String, KafkaFuture<TopicDescription>> topicsDescriptions = adminClient.describeTopics(topics).values();

        int maxReplicationFactor = -1;

        for (Map.Entry<String, KafkaFuture<TopicDescription>> topicDescription : topicsDescriptions.entrySet()) {
            int replicationFactor = getReplicationFactor(topicDescription.getValue());

            if (replicationFactor > maxReplicationFactor) {
                maxReplicationFactor = replicationFactor;
            }
        }

        return maxReplicationFactor;
    }

    private int getReplicationFactor(KafkaFuture<TopicDescription> descriptionKafkaFuture) throws InterruptedException, ExecutionException, TimeoutException {
        return descriptionKafkaFuture.get(timeOut, TimeUnit.MILLISECONDS).partitions().get(0).replicas().size();
    }

}
