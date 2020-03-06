package ru.test.kafka_test.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Set;

@RestController
public class KafkaTestController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private Set<String> topics;

    @GetMapping("/send")
    public String sendMessage() throws InterruptedException {
        for (String topic : topics) {
            kafkaTemplate.send(topic, "message");
        }

        return "sent";
    }

}
