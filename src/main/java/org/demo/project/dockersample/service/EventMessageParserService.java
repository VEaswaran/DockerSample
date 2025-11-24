package org.demo.project.dockersample.service;

import org.demo.project.dockersample.dto.EventData;
import org.demo.project.dockersample.dto.EventMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

/**
 * Service for parsing event messages from Kafka
 */
@Service
public class EventMessageParserService {

    private static final Logger logger = LoggerFactory.getLogger(EventMessageParserService.class);
    private static final String DELIMITER = "\\|";

    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Parses a JSON message string to EventMessage object
     *
     * @param messageContent The raw message content from Kafka
     * @return EventMessage object
     * @throws IllegalArgumentException if parsing fails
     */
    public EventMessage parseMessage(String messageContent) {
        try {
            if (messageContent == null || messageContent.trim().isEmpty()) {
                throw new IllegalArgumentException("Message content is null or empty");
            }

            EventMessage eventMessage = objectMapper.readValue(messageContent, EventMessage.class);

            if (eventMessage == null || eventMessage.getEventIds() == null || eventMessage.getEventIds().isEmpty()) {
                throw new IllegalArgumentException("Parsed message has no eventIds");
            }

            logger.debug("Successfully parsed message with serviceType: {} and {} eventIds",
                eventMessage.getServiceType(), eventMessage.getEventIds().size());

            return eventMessage;
        } catch (IllegalArgumentException e) {
            logger.error("Validation error parsing message: {}", messageContent, e);
            throw e;
        } catch (Exception e) {
            logger.error("Error parsing message content: {}", messageContent, e);
            throw new IllegalArgumentException("Failed to parse message content", e);
        }
    }

    /**
     * Parses event IDs with associated account numbers from the eventIds list
     * Each entry should be in format: eventId|accountNumber
     *
     * @param eventMessage The parsed event message
     * @param originalMessage The original message for reference
     * @return List of parsed EventData objects
     * @throws IllegalArgumentException if parsing fails
     */
    public List<EventData> parseEventIdData(EventMessage eventMessage, String originalMessage) {
        List<EventData> eventDataList = new ArrayList<>();

        if (eventMessage == null || eventMessage.getEventIds() == null) {
            throw new IllegalArgumentException("EventMessage or eventIds is null");
        }

        try {
            String serviceType = eventMessage.getServiceType();
            List<String> eventIds = eventMessage.getEventIds();

            for (String eventIdPair : eventIds) {
                try {
                    String[] parts = eventIdPair.split(DELIMITER);

                    if (parts.length != 2) {
                        logger.warn("Invalid event ID pair format. Expected format: eventId|accountNumber, got: {}",
                            eventIdPair);
                        continue;
                    }

                    String eventId = parts[0].trim();
                    String accountNumber = parts[1].trim();

                    if (eventId.isEmpty() || accountNumber.isEmpty()) {
                        logger.warn("Event ID or account number is empty in pair: {}", eventIdPair);
                        continue;
                    }

                    EventData eventData = EventData.builder()
                            .serviceType(serviceType)
                            .eventId(eventId)
                            .accountNumber(accountNumber)
                            .messageContent(originalMessage)
                            .retryCount(0)
                            .build();

                    eventDataList.add(eventData);

                } catch (Exception e) {
                    logger.error("Error parsing individual event ID pair: {}", eventIdPair, e);
                    // Continue processing other pairs
                }
            }

            if (eventDataList.isEmpty()) {
                throw new IllegalArgumentException("No valid event data parsed from message");
            }

            logger.info("Successfully parsed {} event data entries from message", eventDataList.size());
            return eventDataList;

        } catch (IllegalArgumentException e) {
            logger.error("Validation error parsing event ID data: {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            logger.error("Unexpected error parsing event ID data", e);
            throw new RuntimeException("Failed to parse event ID data", e);
        }
    }
}

