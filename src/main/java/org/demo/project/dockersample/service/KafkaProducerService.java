package org.demo.project.dockersample.service;

import com.fasterxml.jackson.databind.ObjectMapper;
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
    private static final String RETRY_TOPIC = "EM.retrytopic";
    private static final String FALLOUT_TOPIC = "EM.fallout-final";

    @Value("${kafka.topic.name}")
    private String topicName;

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String message) {
        sendToTopic(topicName, message, "default topic");
    }

    public void sendMessageWithKey(String key, String message) {
        logger.info("Sending message with key {} to topic {}", key, topicName);
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, key, message);
        handleSendResult(future, "message with key " + key, topicName);
    }

    public void sendToRetryTopic(String eventDataJson, Integer retryCount) {
        logger.info("Sending event to retry topic (attempt {})", retryCount);
        sendToTopic(RETRY_TOPIC, eventDataJson, "retry topic");
    }

    public void sendToFalloutTopic(String eventDataJson, Integer retryCount) {
        logger.warn("Sending event to fallout topic after {} retries exhausted", retryCount);
        sendToTopic(FALLOUT_TOPIC, eventDataJson, "fallout topic");
    }

    private void sendToTopic(String topic, String message, String description) {
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, message);
        handleSendResult(future, description, topic);
    }

    private void handleSendResult(CompletableFuture<SendResult<String, String>> future, String description, String topic) {
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                logger.info("Message sent successfully to {} - offset: {}", description, result.getRecordMetadata().offset());
            } else {
                logger.error("Failed to send message to {} - topic: {}", description, topic, ex);
            }
        });
    }
}

