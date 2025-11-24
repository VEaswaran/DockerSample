package org.demo.project.dockersample.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.demo.project.dockersample.client.EventRetrievalClient;
import org.demo.project.dockersample.dto.EventData;
import org.demo.project.dockersample.dto.EventMessage;
import org.demo.project.dockersample.dto.RetrieveEventResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * Enhanced Kafka Consumer Service for processing event messages
 * Handles:
 * - Single-threaded message processing
 * - Message parsing and validation
 * - Database existence checks
 * - API retrieval with retry logic
 * - Data insertion with error handling
 * - Manual offset commit for recovery
 */
@Service
public class EventConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(EventConsumerService.class);
    private static final String FALLOUT_TOPIC = "EM.fallouttopic";
    private static final int MAX_INITIAL_RETRIES = 2;

    private final EventMessageParserService parserService;
    private final EventDataService eventDataService;
    private final EventRetrievalClient eventRetrievalClient;
    private final KafkaProducerService kafkaProducerService;
    private final ObjectMapper objectMapper;

    public EventConsumerService(EventMessageParserService parserService,
                               EventDataService eventDataService,
                               EventRetrievalClient eventRetrievalClient,
                               KafkaProducerService kafkaProducerService,
                               ObjectMapper objectMapper) {
        this.parserService = parserService;
        this.eventDataService = eventDataService;
        this.eventRetrievalClient = eventRetrievalClient;
        this.kafkaProducerService = kafkaProducerService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = FALLOUT_TOPIC, groupId = "fallout-event-consumer", concurrency = "1")
    public void consumeEventMessage(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        logger.info("Received message from partition: {}, offset: {}", partition, offset);

        try {
            EventMessage eventMessage = parserService.parseMessage(message);
            List<EventData> eventDataList = parserService.parseEventIdData(eventMessage, message);

            logParsedMessage(eventMessage, eventDataList);
            initializeEventData(eventDataList, partition, offset);

            // Process records with retry logic
            List<EventData> failedRecords = processFirstAttempt(eventDataList);
            failedRecords = retryFailedRecords(failedRecords);
            publishFailedRecordsToRetryTopic(failedRecords);

            acknowledgeOffset(acknowledgment, offset, partition);

        } catch (IllegalArgumentException e) {
            logger.error("Failed to parse message at offset: {}, partition: {}: {}", offset, partition, e.getMessage());
            acknowledgeOffset(acknowledgment, offset, partition);
        } catch (Exception e) {
            logger.error("Unexpected error processing message at offset: {}, partition: {}", offset, partition, e);
            acknowledgeOffset(acknowledgment, offset, partition);
        }
    }

    private void logParsedMessage(EventMessage eventMessage, List<EventData> eventDataList) {
        int eventIdCount = eventMessage.getEventIds() != null ? eventMessage.getEventIds().size() : 0;
        logger.info("Parsed event message - serviceType: {}, eventIdCount: {}, recordCount: {}",
            eventMessage.getServiceType(), eventIdCount, eventDataList.size());
    }

    private void initializeEventData(List<EventData> eventDataList, int partition, long offset) {
        eventDataList.forEach(eventData -> {
            eventData.setPartitionNumber(partition);
            eventData.setOffsetPosition(offset);
            eventData.setRetryCount(0);
        });
    }

    private List<EventData> processFirstAttempt(List<EventData> eventDataList) {
        logger.info("Processing {} records (first attempt)", eventDataList.size());
        List<EventData> failedRecords = new ArrayList<>();

        for (int i = 0; i < eventDataList.size(); i++) {
            EventData eventData = eventDataList.get(i);
            try {
                processEventData(eventData, i + 1, eventDataList.size());
            } catch (Exception e) {
                logger.error("Failed to process record {}/{}: eventId={}, accountNumber={}",
                    i + 1, eventDataList.size(), eventData.getEventId(), eventData.getAccountNumber(), e);
                failedRecords.add(eventData);
            }
        }

        return failedRecords;
    }

    private List<EventData> retryFailedRecords(List<EventData> failedRecords) {
        for (int attempt = 1; attempt <= MAX_INITIAL_RETRIES && !failedRecords.isEmpty(); attempt++) {
            logger.warn("Retrying {} failed records (attempt {}/{})", failedRecords.size(), attempt, MAX_INITIAL_RETRIES);
            List<EventData> stillFailedRecords = new ArrayList<>();

            for (EventData eventData : failedRecords) {
                eventData.setRetryCount(attempt);
                try {
                    processEventData(eventData, 0, 0);
                } catch (Exception e) {
                    logger.error("Failed to retry record: eventId={}, accountNumber={}",
                        eventData.getEventId(), eventData.getAccountNumber(), e);
                    stillFailedRecords.add(eventData);
                }
            }

            failedRecords = stillFailedRecords;
        }
        return failedRecords;
    }

    private void publishFailedRecordsToRetryTopic(List<EventData> failedRecords) {
        if (failedRecords.isEmpty()) {
            logger.info("All records processed successfully");
            return;
        }

        logger.warn("Publishing {} failed records to retry topic", failedRecords.size());
        for (EventData eventData : failedRecords) {
            try {
                eventData.setRetryCount(MAX_INITIAL_RETRIES);
                String eventDataJson = objectMapper.writeValueAsString(eventData);
                kafkaProducerService.sendToRetryTopic(eventDataJson, MAX_INITIAL_RETRIES);
                logger.info("Published to retry topic: eventId={}, accountNumber={}",
                    eventData.getEventId(), eventData.getAccountNumber());
            } catch (Exception e) {
                logger.error("Failed to publish event to retry topic: eventId={}, accountNumber={}",
                    eventData.getEventId(), eventData.getAccountNumber(), e);
            }
        }
    }

    private void acknowledgeOffset(Acknowledgment acknowledgment, long offset, int partition) {
        if (acknowledgment != null) {
            acknowledgment.acknowledge();
            logger.info("Acknowledged offset: {} for partition: {}", offset, partition);
        }
    }

    /**
     * Processes a single event data record
     *
     * @param eventData The event data to process
     * @param recordIndex Current record index for logging
     * @param totalRecords Total records in the batch
     */
    private void processEventData(EventData eventData, int recordIndex, int totalRecords) {
        logger.debug("Processing record {}/{}: eventId={}, accountNumber={}",
            recordIndex, totalRecords, eventData.getEventId(), eventData.getAccountNumber());

        try {
            // Step 1: Check if data exists in database
            boolean dataExists = eventDataService.eventDataExists(eventData.getEventId(), eventData.getAccountNumber());

            if (dataExists) {
                logger.info("Record {}/{}: Data already exists in database for eventId: {}, accountNumber: {} - SKIPPING",
                    recordIndex, totalRecords, eventData.getEventId(), eventData.getAccountNumber());
                return;
            }

            // Step 2: Data not found, retrieve from API with retry logic
            logger.info("Record {}/{}: Data not found in database for eventId: {}, accountNumber: {} - RETRIEVING from API",
                recordIndex, totalRecords, eventData.getEventId(), eventData.getAccountNumber());

            RetrieveEventResponse eventResponse;
            try {
                eventResponse = eventRetrievalClient.retrieveEventData(eventData.getEventId());
            } catch (RuntimeException e) {
                logger.error("Record {}/{}: Failed to retrieve data from API after retries for eventId: {}, accountNumber: {}",
                    recordIndex, totalRecords, eventData.getEventId(), eventData.getAccountNumber(), e);
                throw e;
            }

            // Step 3: Insert retrieved data to database
            if (eventResponse != null) {
                try {
                    boolean insertSuccess = eventDataService.insertEventData(eventResponse);

                    if (insertSuccess) {
                        logger.info("Record {}/{}: Successfully inserted event data for eventId: {}, accountNumber: {}",
                            recordIndex, totalRecords, eventData.getEventId(), eventData.getAccountNumber());
                    } else {
                        logger.warn("Record {}/{}: Database insert returned false for eventId: {}, accountNumber: {}",
                            recordIndex, totalRecords, eventData.getEventId(), eventData.getAccountNumber());
                    }
                } catch (Exception e) {
                    logger.error("Record {}/{}: Failed to insert event data for eventId: {}, accountNumber: {}",
                        recordIndex, totalRecords, eventData.getEventId(), eventData.getAccountNumber(), e);
                    throw new RuntimeException("Failed to insert event data", e);
                }
            } else {
                logger.warn("Record {}/{}: API returned null response for eventId: {}, accountNumber: {}",
                    recordIndex, totalRecords, eventData.getEventId(), eventData.getAccountNumber());
            }

        } catch (RuntimeException e) {
            // Re-throw runtime exceptions for proper error handling
            throw e;
        } catch (Exception e) {
            logger.error("Record {}/{}: Unexpected error processing eventId: {}, accountNumber: {}",
                recordIndex, totalRecords, eventData.getEventId(), eventData.getAccountNumber(), e);
            throw new RuntimeException("Unexpected error processing event data", e);
        }
    }
}

