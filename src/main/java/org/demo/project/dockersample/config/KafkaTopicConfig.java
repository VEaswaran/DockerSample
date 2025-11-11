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
}

