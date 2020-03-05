//package ru.test.kafka_test.kafka.health;
//
//import org.apache.kafka.clients.admin.*;
//import org.apache.kafka.common.KafkaFuture;
//import org.apache.kafka.common.Node;
//import org.apache.kafka.common.TopicPartitionInfo;
//import org.apache.kafka.common.config.ConfigResource;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import org.springframework.boot.actuate.health.Health;
//import org.springframework.boot.actuate.health.Status;
//
//import java.util.*;
//import java.util.concurrent.ExecutionException;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.TimeoutException;
//
//import static org.junit.Assert.assertEquals;
//import static org.mockito.ArgumentMatchers.*;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.when;
//
//class KafkaHealthIndicatorTest {
//
//    private static final long TIME_OUT = 100;
//
//    private KafkaHealthIndicator kafkaHealthIndicator;
//
//    private AdminClient adminClient;
//
//    @BeforeEach
//    public void init() {
//        this.adminClient = mock(AdminClient.class);
//        this.kafkaHealthIndicator = new KafkaHealthIndicator(adminClient, new HashSet<>(), TIME_OUT);
//    }
//
//    private KafkaFuture<TopicDescription> mockDescriptionKafkaFuture(int replicationFactor) throws InterruptedException, ExecutionException, TimeoutException {
//        KafkaFuture<TopicDescription> kafkaFuture = mock(KafkaFuture.class);
//        TopicDescription topicDescription = mock(TopicDescription.class);
//        TopicPartitionInfo topicPartitionInfo = mock(TopicPartitionInfo.class);
//        List<TopicPartitionInfo> topicPartitionInfoList = Collections.singletonList(topicPartitionInfo);
//        List<Node> replicas = mock(ArrayList.class);
//        when(replicas.size()).thenReturn(replicationFactor);
//        when(topicPartitionInfo.replicas()).thenReturn(replicas);
//        when(topicDescription.partitions()).thenReturn(topicPartitionInfoList);
//        when(kafkaFuture.get(TIME_OUT, TimeUnit.MILLISECONDS)).thenReturn(topicDescription);
//
//        return kafkaFuture;
//    }
//
//    private void mockDescribeTopics(Map<String, KafkaFuture<TopicDescription>> topicsDescriptions) {
//        DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);
//        when(describeTopicsResult.values()).thenReturn(topicsDescriptions);
//        when(adminClient.describeTopics(anySet())).thenReturn(describeTopicsResult);
//    }
//
//    private void mockDescribeClusterResult(String id, int size) throws InterruptedException, ExecutionException, TimeoutException {
//        DescribeClusterResult describeClusterResult = mock(DescribeClusterResult.class);
//
//        KafkaFuture<String> kafkaFutureClusterId = mock(KafkaFuture.class);
//        when(kafkaFutureClusterId.get(TIME_OUT, TimeUnit.MILLISECONDS)).thenReturn(id);
//        when(describeClusterResult.clusterId()).thenReturn(kafkaFutureClusterId);
//
//        KafkaFuture<Collection<Node>> kafkaFutureNodes = mock(KafkaFuture.class);
//        when(kafkaFutureNodes.get(TIME_OUT, TimeUnit.MILLISECONDS)).thenReturn(initNodes(size));
//        when(describeClusterResult.nodes()).thenReturn(kafkaFutureNodes);
//
//        when(adminClient.describeCluster(any(DescribeClusterOptions.class))).thenReturn(describeClusterResult);
//    }
//
//    private List<Node> initNodes(int size) {
//        List<Node> nodeList = new ArrayList<>();
//        for (int i = 0; i < size; i++) {
//            nodeList.add(new Node(1, "host", 666));
//        }
//        return nodeList;
//    }
//
//    private void mockDescribeConfigsResult(String brokerId) throws InterruptedException, ExecutionException, TimeoutException {
//        DescribeConfigsResult describeConfigsResult = mock(DescribeConfigsResult.class);
//        when(adminClient.describeConfigs(anyCollection())).thenReturn(describeConfigsResult);
//        KafkaFuture<Map<ConfigResource, Config>> kafkaFuture = mock(KafkaFuture.class);
//        Map<ConfigResource, Config> configResourceConfigMap = mock(HashMap.class);
//        Config config = mock(Config.class);
//        ConfigEntry configEntry = mock(ConfigEntry.class);
//        when(configEntry.value()).thenReturn(brokerId);
//        when(config.get("broker.id")).thenReturn(configEntry);
//        when(configResourceConfigMap.get(any())).thenReturn(config);
//        when(kafkaFuture.get(TIME_OUT, TimeUnit.MILLISECONDS)).thenReturn(configResourceConfigMap);
//        when(describeConfigsResult.all()).thenReturn(kafkaFuture);
//    }
//
//
//    @Test
//    public void healthUp() throws InterruptedException, ExecutionException, TimeoutException {
//        int replicationFactor = 3, nodes = 3;
//        String brokerId = "123", id = "id";
//
//        mockDescribeTopics(getTopicDescriptionsMapMock(replicationFactor));
//        mockDescribeClusterResult(id, nodes);
//        mockDescribeConfigsResult(brokerId);
//
//        Health result = kafkaHealthIndicator.health();
//
//        assertEquals(Status.UP, result.getStatus());
//        assertEquals(String.valueOf(replicationFactor), result.getDetails().get("replicationFactor"));
//        assertEquals(id, result.getDetails().get("clusterId"));
//        assertEquals(String.valueOf(nodes), result.getDetails().get("nodes"));
//        assertEquals(brokerId, result.getDetails().get("brokerId"));
//    }
//
//    @Test
//    public void healthDown() throws InterruptedException, ExecutionException, TimeoutException {
//        int replicationFactor = 3, nodes = 2;
//        String brokerId = "123", id = "id";
//
//        mockDescribeTopics(getTopicDescriptionsMapMock(replicationFactor));
//        mockDescribeClusterResult(id, nodes);
//        mockDescribeConfigsResult(brokerId);
//
//        Health result = kafkaHealthIndicator.health();
//
//        assertEquals(Status.DOWN, result.getStatus());
//        assertEquals(String.valueOf(replicationFactor), result.getDetails().get("replicationFactor"));
//        assertEquals(id, result.getDetails().get("clusterId"));
//        assertEquals(String.valueOf(nodes), result.getDetails().get("nodes"));
//        assertEquals(brokerId, result.getDetails().get("brokerId"));
//    }
//
//    private Map<String, KafkaFuture<TopicDescription>> getTopicDescriptionsMapMock(int replicationFactor) throws InterruptedException, ExecutionException, TimeoutException {
//        Map<String, KafkaFuture<TopicDescription>> topicsDescriptions = new HashMap<>();
//        for (int i = 1; i <= replicationFactor; i++) {
//            topicsDescriptions.put("Topic_" + i, mockDescriptionKafkaFuture(i));
//        }
//        return topicsDescriptions;
//    }
//
//}