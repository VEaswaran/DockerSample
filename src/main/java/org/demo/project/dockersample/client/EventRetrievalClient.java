package org.demo.project.dockersample.client;

import org.demo.project.dockersample.dto.RetrieveEventResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

@Component
public class EventRetrievalClient {

    private static final Logger logger = LoggerFactory.getLogger(EventRetrievalClient.class);

    private static final int MAX_RETRIES = 5;
    private static final long RETRY_WAIT_MS = 1000; // 1 second

    @Value("${event.api.base-url:http://localhost:8080}")
    private String baseUrl;

    private final RestTemplate restTemplate;

    public EventRetrievalClient(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    /**
     * Retrieves event data from the API with retry logic
     *
     * @param eventId The event ID to retrieve
     * @return RetrieveEventResponse with event data
     * @throws RuntimeException if all retries fail
     */
    public RetrieveEventResponse retrieveEventData(String eventId) {
        int retryCount = 0;
        Exception lastException = null;

        while (retryCount < MAX_RETRIES) {
            try {
                String url = String.format("%s/mspp/retrieve/eventid/%s", baseUrl, eventId);
                logger.debug("Attempting to retrieve event data from API: {}, retry: {}/{}",
                    url, retryCount + 1, MAX_RETRIES);

                RetrieveEventResponse response = restTemplate.getForObject(url, RetrieveEventResponse.class);

                if (response != null) {
                    logger.info("Successfully retrieved event data for eventId: {}", eventId);
                    return response;
                } else {
                    logger.warn("API returned null response for eventId: {}", eventId);
                    lastException = new RuntimeException("API returned null response");
                }
            } catch (RestClientException e) {
                lastException = e;
                retryCount++;

                if (retryCount < MAX_RETRIES) {
                    logger.warn("API call failed for eventId: {}, retrying {} of {}: {}",
                        eventId, retryCount + 1, MAX_RETRIES, e.getMessage());

                    try {
                        Thread.sleep(RETRY_WAIT_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        logger.error("Thread interrupted during retry wait", ie);
                        throw new RuntimeException("Thread interrupted during API retry", ie);
                    }
                } else {
                    logger.error("API call failed after {} retries for eventId: {}", MAX_RETRIES, eventId, e);
                }
            } catch (Exception e) {
                lastException = e;
                retryCount++;

                if (retryCount < MAX_RETRIES) {
                    logger.warn("Unexpected error during API call for eventId: {}, retrying {} of {}: {}",
                        eventId, retryCount + 1, MAX_RETRIES, e.getMessage());

                    try {
                        Thread.sleep(RETRY_WAIT_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        logger.error("Thread interrupted during retry wait", ie);
                        throw new RuntimeException("Thread interrupted during API retry", ie);
                    }
                } else {
                    logger.error("Unexpected error after {} retries for eventId: {}", MAX_RETRIES, eventId, e);
                }
            }
        }

        throw new RuntimeException(
            String.format("Failed to retrieve event data for eventId: %s after %d retries",
                eventId, MAX_RETRIES),
            lastException
        );
    }
}

