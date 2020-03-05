package ru.test.kafka_test.kafka.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ru.test.kafka_test.kafka.KafkaTopicConfig;

@Component
public class KafkaConsumer {

    private static final String GROUP_1 = "group1";

    private String message = "nothing received";

    @KafkaListener(topics = KafkaTopicConfig.TOPIC_NAME, groupId = GROUP_1)
    public void listen(String message) {
        this.message = message;
        System.out.println("Received Message in group " + GROUP_1 + ": " + message);
    }

    public String getMessage() {
        String bufferedMessage = message;
        message = "nothing received";

        return bufferedMessage;
    }
}
