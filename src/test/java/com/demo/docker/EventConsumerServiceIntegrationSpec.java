package com.demo.docker;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.demo.project.dockersample.dto.EventMessage;
import org.demo.project.dockersample.service.EventConsumerService;
import org.demo.project.dockersample.service.EventMessageParserService;
import org.demo.project.dockersample.service.EventDataService;
import org.demo.project.dockersample.client.EventRetrievalClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@EmbeddedKafka(partitions = 1, brokerProperties = { "listeners=PLAINTEXT://localhost:29092", "port=29092" })
@TestPropertySource(properties = {
    "spring.kafka.bootstrap-servers=localhost:29092",
    "kafka.fallout.topic.name=EM.fallouttopic"
})
@DisplayName("Event Consumer Service Tests")
public class EventConsumerServiceIntegrationSpec {

    @Autowired
    private EventMessageParserService parserService;

    @Autowired
    private EventDataService eventDataService;

    @Autowired
    private EventConsumerService eventConsumerService;

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
    }

    @Test
    @DisplayName("Should parse valid event message correctly")
    void testParseValidEventMessage() {
        // Given
        String message = "{\"serviceType\":\"wire\",\"eventIds\":[\"event-001|ACC-12345\",\"event-002|ACC-12346\"]}";

        // When
        EventMessage result = parserService.parseMessage(message);

        // Then
        assertNotNull(result);
        assertEquals("wire", result.getServiceType());
        assertEquals(2, result.getEventIds().size());
        assertTrue(result.getEventIds().contains("event-001|ACC-12345"));
    }

    @Test
    @DisplayName("Should throw exception for invalid JSON message")
    void testParseInvalidJsonMessage() {
        // Given
        String invalidMessage = "{invalid json}";

        // When & Then
        assertThrows(IllegalArgumentException.class, () -> {
            parserService.parseMessage(invalidMessage);
        });
    }

    @Test
    @DisplayName("Should throw exception for empty eventIds")
    void testParseMessageWithEmptyEventIds() {
        // Given
        String message = "{\"serviceType\":\"wire\",\"eventIds\":[]}";

        // When & Then
        assertThrows(IllegalArgumentException.class, () -> {
            parserService.parseMessage(message);
        });
    }

    @Test
    @DisplayName("Should parse event ID data with account numbers correctly")
    void testParseEventIdData() {
        // Given
        String message = "{\"serviceType\":\"wire\",\"eventIds\":[\"event-001|ACC-12345\",\"event-002|ACC-12346\"]}";
        EventMessage eventMessage = parserService.parseMessage(message);

        // When
        var eventDataList = parserService.parseEventIdData(eventMessage, message);

        // Then
        assertNotNull(eventDataList);
        assertEquals(2, eventDataList.size());
        assertEquals("event-001", eventDataList.get(0).getEventId());
        assertEquals("ACC-12345", eventDataList.get(0).getAccountNumber());
        assertEquals("event-002", eventDataList.get(1).getEventId());
        assertEquals("ACC-12346", eventDataList.get(1).getAccountNumber());
    }

    @Test
    @DisplayName("Should skip invalid event ID pairs")
    void testParseEventIdDataWithInvalidPairs() {
        // Given
        String message = "{\"serviceType\":\"wire\",\"eventIds\":[\"event-001|ACC-12345\",\"invalid-pair\",\"event-002|ACC-12346\"]}";
        EventMessage eventMessage = parserService.parseMessage(message);

        // When
        var eventDataList = parserService.parseEventIdData(eventMessage, message);

        // Then
        assertNotNull(eventDataList);
        // Should skip the invalid pair
        assertEquals(2, eventDataList.size());
    }

    @Test
    @DisplayName("Should check if event data exists in database")
    void testEventDataExists() {
        // Given
        String eventId = "event-001";
        String accountNumber = "ACC-12345";

        // When - data doesn't exist initially
        boolean existsInitially = eventDataService.eventDataExists(eventId, accountNumber);

        // Then
        assertFalse(existsInitially);
    }

    @Test
    @DisplayName("Should insert and retrieve event data from database")
    void testInsertAndRetrieveEventData() {
        // Given
        String eventId = "event-001";
        String accountNumber = "ACC-12345";
        var eventResponse = org.demo.project.dockersample.dto.RetrieveEventResponse.builder()
                .eventId(eventId)
                .accountNumber(accountNumber)
                .eventData("sample event data")
                .status("success")
                .build();

        // When
        boolean insertResult = eventDataService.insertEventData(eventResponse);

        // Then
        assertTrue(insertResult);
        assertTrue(eventDataService.eventDataExists(eventId, accountNumber));

        var retrieved = eventDataService.getEventData(eventId, accountNumber);
        assertNotNull(retrieved);
        assertEquals(eventId, retrieved.getEventId());
        assertEquals(accountNumber, retrieved.getAccountNumber());
    }
}

