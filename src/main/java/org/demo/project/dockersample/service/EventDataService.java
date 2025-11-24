package org.demo.project.dockersample.service;

import java.util.HashMap;
import java.util.Map;
import org.demo.project.dockersample.dto.RetrieveEventResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Service for database operations related to event data
 * This is a placeholder implementation. Replace with actual database operations.
 */
@Service
public class EventDataService {

    private static final Logger logger = LoggerFactory.getLogger(EventDataService.class);

    // In-memory store for demonstration purposes
    // Replace this with actual database operations
    private final Map<String, RetrieveEventResponse> eventDataStore = new HashMap<>();

    /**
     * Checks if event data exists in the database
     *
     * @param eventId The event ID to check
     * @param accountNumber The account number associated with the event
     * @return true if data exists, false otherwise
     */
    public boolean eventDataExists(String eventId, String accountNumber) {
        try {
            String key = generateKey(eventId, accountNumber);
            boolean exists = eventDataStore.containsKey(key);

            if (exists) {
                logger.debug("Event data found in database for eventId: {}, accountNumber: {}",
                    eventId, accountNumber);
            } else {
                logger.debug("Event data NOT found in database for eventId: {}, accountNumber: {}",
                    eventId, accountNumber);
            }

            return exists;
        } catch (Exception e) {
            logger.error("Error checking if event data exists for eventId: {}, accountNumber: {}",
                eventId, accountNumber, e);
            throw new RuntimeException("Database query failed", e);
        }
    }

    /**
     * Inserts or updates event data in the database
     *
     * @param eventResponse The event response to insert/update
     * @return true if insertion/update was successful
     */
    public boolean insertEventData(RetrieveEventResponse eventResponse) {
        try {
            if (eventResponse == null) {
                logger.warn("Attempted to insert null event response");
                return false;
            }

            String key = generateKey(eventResponse.getEventId(), eventResponse.getAccountNumber());
            eventDataStore.put(key, eventResponse);

            logger.info("Successfully inserted/updated event data for eventId: {}, accountNumber: {}",
                eventResponse.getEventId(), eventResponse.getAccountNumber());

            return true;
        } catch (Exception e) {
            logger.error("Error inserting event data for eventId: {}, accountNumber: {}",
                eventResponse != null ? eventResponse.getEventId() : "NULL",
                eventResponse != null ? eventResponse.getAccountNumber() : "NULL",
                e);
            throw new RuntimeException("Database insert failed", e);
        }
    }

    /**
     * Retrieves event data from the database
     *
     * @param eventId The event ID to retrieve
     * @param accountNumber The account number associated with the event
     * @return RetrieveEventResponse if found, null otherwise
     */
    public RetrieveEventResponse getEventData(String eventId, String accountNumber) {
        try {
            String key = generateKey(eventId, accountNumber);
            RetrieveEventResponse response = eventDataStore.get(key);

            if (response != null) {
                logger.debug("Retrieved event data for eventId: {}, accountNumber: {}",
                    eventId, accountNumber);
            }

            return response;
        } catch (Exception e) {
            logger.error("Error retrieving event data for eventId: {}, accountNumber: {}",
                eventId, accountNumber, e);
            throw new RuntimeException("Database retrieval failed", e);
        }
    }

    /**
     * Generates a composite key for event identification
     *
     * @param eventId The event ID
     * @param accountNumber The account number
     * @return Composite key
     */
    private String generateKey(String eventId, String accountNumber) {
        return eventId + "_" + accountNumber;
    }
}

