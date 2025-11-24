package org.demo.project.dockersample.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    private static final Logger logger = LoggerFactory.getLogger(KafkaTopicConfig.class);

    @Value("${kafka.topic.name}")
    private String topicName;

    @Bean
    public NewTopic dockerMessageTopic() {
        logger.info("Creating Kafka topic bean: {}", topicName);
        try {
            return TopicBuilder.name(topicName)
                    .partitions(3)
                    .replicas(1)
                    .build();
        } catch (Exception e) {
            logger.warn("Failed to create Kafka topic bean: {}", e.getMessage());
            throw e;
        }
    }

    @Bean
    public NewTopic retryEventTopic() {
        String retryTopicName = "EM.retrytopic";
        logger.info("Creating Kafka retry topic bean: {}", retryTopicName);
        try {
            return TopicBuilder.name(retryTopicName)
                    .partitions(3)
                    .replicas(1)
                    .build();
        } catch (Exception e) {
            logger.warn("Failed to create Kafka retry topic bean: {}", e.getMessage());
            throw e;
        }
    }

    @Bean
    public NewTopic falloutEventTopic() {
        String falloutTopicName = "EM.fallout-final";
        logger.info("Creating Kafka fallout topic bean: {}", falloutTopicName);
        try {
            return TopicBuilder.name(falloutTopicName)
                    .partitions(3)
                    .replicas(1)
                    .build();
        } catch (Exception e) {
            logger.warn("Failed to create Kafka fallout topic bean: {}", e.getMessage());
            throw e;
        }
    }
}

