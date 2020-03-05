package ru.test.kafka_test.controller;

import org.apache.kafka.common.protocol.types.Field;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.test.kafka_test.kafka.consumer.KafkaConsumer;
import ru.test.kafka_test.kafka.producer.KafkaProducer;

import java.util.Random;

@RestController
public class KafkaTestController {

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private KafkaConsumer kafkaConsumer;

    @GetMapping("/send")
    public String sendMessage() throws InterruptedException {
        Thread.sleep(500);
        String message = "vasya molodec " + Math.random();
        kafkaProducer.sendMessage(message);

        Thread.sleep(500);

        String receivedMessage = kafkaConsumer.getMessage();

        return "Message  [" + message + "] sent" + "\n " +
                "Message [" + receivedMessage + "] received";
    }

    @GetMapping("/donothing")
    public String doNothing(){
        System.out.println("Nothing done");
        return "Nothing done";
    }



}
