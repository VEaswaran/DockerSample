package org.demo.project.dockersample.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.demo.project.dockersample.client.EventRetrievalClient;
import org.demo.project.dockersample.dto.EventData;
import org.demo.project.dockersample.dto.RetrieveEventResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

/**
 * Kafka Consumer Service for processing retry events
 * Handles retried event records from the retry topic
 * Applies exponential backoff and publishes to fallout topic after max retries
 */
@Service
public class RetryEventConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(RetryEventConsumerService.class);
    private static final String RETRY_TOPIC = "EM.retrytopic";
    private static final int MAX_RETRY_COUNT = 5;

    private final EventDataService eventDataService;
    private final EventRetrievalClient eventRetrievalClient;
    private final KafkaProducerService kafkaProducerService;
    private final ObjectMapper objectMapper;

    public RetryEventConsumerService(EventDataService eventDataService,
                                     EventRetrievalClient eventRetrievalClient,
                                     KafkaProducerService kafkaProducerService,
                                     ObjectMapper objectMapper) {
        this.eventDataService = eventDataService;
        this.eventRetrievalClient = eventRetrievalClient;
        this.kafkaProducerService = kafkaProducerService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = RETRY_TOPIC, groupId = "retry-event-consumer", concurrency = "1")
    public void consumeRetryEventMessage(
            @Payload String eventDataJson,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        logger.info("Received retry message from partition: {}, offset: {}", partition, offset);

        try {
            EventData eventData = objectMapper.readValue(eventDataJson, EventData.class);
            int currentRetryCount = eventData.getRetryCount() != null ? eventData.getRetryCount() : 0;
            int newRetryCount = currentRetryCount + 1;

            logger.info("Processing retry event: eventId={}, accountNumber={}, attempt={}/{}",
                eventData.getEventId(), eventData.getAccountNumber(), newRetryCount, MAX_RETRY_COUNT);

            handleRetryEvent(eventData, newRetryCount);
            acknowledgeOffset(acknowledgment, offset, partition);

        } catch (Exception e) {
            logger.error("Error processing retry message at offset: {}, partition: {}", offset, partition, e);
            acknowledgeOffset(acknowledgment, offset, partition);
        }
    }

    private void handleRetryEvent(EventData eventData, int newRetryCount) throws Exception {
        if (processRetryEventData(eventData)) {
            logger.info("Retry event succeeded: eventId={}, accountNumber={}, attempt={}",
                eventData.getEventId(), eventData.getAccountNumber(), newRetryCount);
            return;
        }

        if (newRetryCount < MAX_RETRY_COUNT) {
            republishToRetryTopic(eventData, newRetryCount);
        } else {
            publishToFalloutTopic(eventData, newRetryCount);
        }
    }

    private void republishToRetryTopic(EventData eventData, int newRetryCount) throws Exception {
        eventData.setRetryCount(newRetryCount);
        long backoffMs = calculateBackoffDelay(newRetryCount);

        logger.warn("Retry failed, will retry again: eventId={}, accountNumber={}, nextAttempt={}/{}, backoffDelay={}ms",
            eventData.getEventId(), eventData.getAccountNumber(), newRetryCount + 1, MAX_RETRY_COUNT, backoffMs);

        String eventDataJson = objectMapper.writeValueAsString(eventData);
        kafkaProducerService.sendToRetryTopic(eventDataJson, newRetryCount + 1);
    }

    private void publishToFalloutTopic(EventData eventData, int newRetryCount) throws Exception {
        logger.error("Max retries ({}) exhausted: eventId={}, accountNumber={}, sending to fallout topic",
            MAX_RETRY_COUNT, eventData.getEventId(), eventData.getAccountNumber());

        eventData.setRetryCount(MAX_RETRY_COUNT);
        String eventDataJson = objectMapper.writeValueAsString(eventData);
        kafkaProducerService.sendToFalloutTopic(eventDataJson, MAX_RETRY_COUNT);
    }

    private void acknowledgeOffset(Acknowledgment acknowledgment, long offset, int partition) {
        if (acknowledgment != null) {
            acknowledgment.acknowledge();
            logger.info("Acknowledged offset: {} for partition: {}", offset, partition);
        }
    }

    private boolean processRetryEventData(EventData eventData) {
        try {
            String eventId = eventData.getEventId();
            String accountNumber = eventData.getAccountNumber();

            // Check if data already exists in database
            if (eventDataService.eventDataExists(eventId, accountNumber)) {
                logger.info("Data already exists in database: eventId={}, accountNumber={} - SKIPPING",
                    eventId, accountNumber);
                return true;
            }

            // Retrieve data from API
            RetrieveEventResponse eventResponse = retrieveEventFromApi(eventId, accountNumber);
            if (eventResponse == null) {
                return false;
            }

            // Insert to database
            return insertEventToDatabase(eventResponse, eventId, accountNumber);

        } catch (Exception e) {
            logger.error("Error processing retry event: eventId={}, accountNumber={}",
                eventData.getEventId(), eventData.getAccountNumber(), e);
            return false;
        }
    }

    private RetrieveEventResponse retrieveEventFromApi(String eventId, String accountNumber) {
        try {
            logger.info("Retrieving event from API: eventId={}, accountNumber={}", eventId, accountNumber);
            return eventRetrievalClient.retrieveEventData(eventId);
        } catch (Exception e) {
            logger.error("Failed to retrieve event from API: eventId={}, accountNumber={}", eventId, accountNumber, e);
            return null;
        }
    }

    private boolean insertEventToDatabase(RetrieveEventResponse eventResponse, String eventId, String accountNumber) {
        try {
            boolean insertSuccess = eventDataService.insertEventData(eventResponse);
            if (insertSuccess) {
                logger.info("Successfully inserted event data: eventId={}, accountNumber={}", eventId, accountNumber);
            } else {
                logger.warn("Database insert returned false: eventId={}, accountNumber={}", eventId, accountNumber);
            }
            return insertSuccess;
        } catch (Exception e) {
            logger.error("Failed to insert event data: eventId={}, accountNumber={}", eventId, accountNumber, e);
            return false;
        }
    }

    /**
     * Calculates exponential backoff delay in milliseconds
     * Formula: min(2^retryAttempt * 100ms, 30000ms)
     *
     * @param retryAttempt The retry attempt number (1-based)
     * @return Backoff delay in milliseconds
     */
    private long calculateBackoffDelay(int retryAttempt) {
        long baseDelay = 100; // 100ms base delay
        long exponentialDelay = baseDelay * (long) Math.pow(2, retryAttempt);
        long maxDelay = 30000; // Cap at 30 seconds
        return Math.min(exponentialDelay, maxDelay);
    }
}

