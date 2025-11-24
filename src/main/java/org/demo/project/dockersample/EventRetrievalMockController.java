package org.demo.project.dockersample;

import org.demo.project.dockersample.dto.RetrieveEventResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Mock API Controller for testing the consumer
 * This simulates the external API endpoint: /mspp/retrieve/eventid/{eventId}
 *
 * In production, this would be replaced with actual API calls to the real endpoint
 */
@RestController
@RequestMapping("/mspp")
public class EventRetrievalMockController {

    private static final Logger logger = LoggerFactory.getLogger(EventRetrievalMockController.class);

    /**
     * Mock endpoint for retrieving event data
     *
     * @param eventId The event ID to retrieve
     * @return RetrieveEventResponse with mock data
     */
    @GetMapping("/retrieve/eventid/{eventId}")
    public RetrieveEventResponse retrieveEventData(@PathVariable String eventId) {
        logger.info("Mock API received request for eventId: {}", eventId);

        // Simulate some processing time
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Return mock response
        RetrieveEventResponse response = RetrieveEventResponse.builder()
                .eventId(eventId)
                .accountNumber("MOCK-ACC-" + System.nanoTime())
                .eventData("Mock event data for " + eventId)
                .status("success")
                .build();

        logger.info("Mock API returning response for eventId: {}", eventId);
        return response;
    }
}

