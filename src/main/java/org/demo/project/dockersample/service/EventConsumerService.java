package org.demo.project.dockersample.service;

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

    private final EventMessageParserService parserService;
    private final EventDataService eventDataService;
    private final EventRetrievalClient eventRetrievalClient;

    public EventConsumerService(EventMessageParserService parserService,
                               EventDataService eventDataService,
                               EventRetrievalClient eventRetrievalClient) {
        this.parserService = parserService;
        this.eventDataService = eventDataService;
        this.eventRetrievalClient = eventRetrievalClient;
    }

    /**
     * Consumes messages from the fallout topic
     * Processes one message at a time with manual offset management
     *
     * @param message The message content
     * @param partition The partition number
     * @param offset The offset position
     * @param acknowledgment Manual acknowledgment handler
     */
    @KafkaListener(topics = FALLOUT_TOPIC, groupId = "fallout-event-consumer", concurrency = "1")
    public void consumeEventMessage(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        logger.info("Received message from partition: {}, offset: {}", partition, offset);

        try {
            // Parse the JSON message
            EventMessage eventMessage = parserService.parseMessage(message);
            int eventIdCount = eventMessage.getEventIds() != null ? eventMessage.getEventIds().size() : 0;
            logger.info("Parsed event message with serviceType: {}, eventIdCount: {}",
                eventMessage.getServiceType(), eventIdCount);

            // Parse individual event ID and account number pairs
            List<EventData> eventDataList = parserService.parseEventIdData(eventMessage, message);

            // Process each event data record
            for (int i = 0; i < eventDataList.size(); i++) {
                EventData eventData = eventDataList.get(i);
                eventData.setPartitionNumber(partition);
                eventData.setOffsetPosition(offset);

                try {
                    processEventData(eventData, i + 1, eventDataList.size());
                } catch (Exception e) {
                    logger.error("Error processing eventData at index {}/{}: eventId={}, accountNumber={}, partition={}, offset={}",
                        i + 1, eventDataList.size(), eventData.getEventId(), eventData.getAccountNumber(),
                        partition, offset, e);
                    // Continue processing other records in the batch
                }
            }

            // Acknowledge the offset only after successful processing of all records
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
                logger.info("Successfully acknowledged offset: {} for partition: {}", offset, partition);
            }

        } catch (IllegalArgumentException e) {
            logger.error("Failed to parse message at offset: {}, partition: {}: {}",
                offset, partition, e.getMessage());
            // Skip to next message on parse error
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
            }
        } catch (Exception e) {
            logger.error("Unexpected error processing message at offset: {}, partition: {}",
                offset, partition, e);
            // Acknowledge to avoid infinite loop on unexpected errors
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
            }
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

