package org.demo.project.dockersample.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaProducerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerService.class);

    @Value("${kafka.topic.name}")
    private String topicName;

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String message) {
        logger.info("Sending message to topic {}: {}", topicName, message);

        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, message);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                logger.info("Message sent successfully to topic {}: {} with offset: {}",
                        topicName, message, result.getRecordMetadata().offset());
            } else {
                logger.error("Failed to send message to topic {}: {}", topicName, message, ex);
            }
        });
    }

    public void sendMessageWithKey(String key, String message) {
        logger.info("Sending message with key {} to topic {}: {}", key, topicName, message);

        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, key, message);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                logger.info("Message with key {} sent successfully to topic {}: {} with offset: {}",
                        key, topicName, message, result.getRecordMetadata().offset());
            } else {
                logger.error("Failed to send message with key {} to topic {}: {}", key, topicName, message, ex);
            }
        });
    }
}

