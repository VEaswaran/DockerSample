# Event Fallout Consumer Implementation

This document explains the implementation of a robust Kafka consumer for processing event messages from the "EM.fallouttopic" topic.

## Architecture Overview

The implementation follows a layered architecture with clear separation of concerns:

```
┌─────────────────────────────────────────────────────────────┐
│                  Event Consumer Service                     │
│         (Main orchestration and message processing)         │
└────────────────┬──────────────────────────────────────────┘
                 │
    ┌────────────┼────────────┐
    │            │            │
    ▼            ▼            ▼
┌─────────┐ ┌────────┐ ┌──────────────┐
│ Parser  │ │ Data   │ │ Retrieval    │
│ Service │ │Service │ │ Client       │
└─────────┘ └────────┘ └──────────────┘
                │            │
                │            ▼
                ▼      ┌──────────────┐
           Database   │  External API │
                      └──────────────┘
```

## Components

### 1. **KafkaConsumerConfig** (`config/KafkaConsumerConfig.java`)
Configures the Kafka consumer with:
- **Single-threaded processing**: `concurrency = 1`
- **Single message at a time**: `MAX_POLL_RECORDS_CONFIG = 1`
- **Manual offset management**: For recovery from failures
- **Auto-commit disabled**: Allows precise control over offset commits

### 2. **EventMessageParserService** (`service/EventMessageParserService.java`)
Responsible for parsing messages:
- Parses JSON message format: `{ "serviceType": "wire", "eventIds": [...] }`
- Extracts individual event IDs with account numbers using `|` delimiter
- Returns structured `EventData` objects for processing
- Comprehensive error handling for malformed messages

### 3. **EventConsumerService** (`service/EventConsumerService.java`)
Main consumer orchestration:
- Listens on `EM.fallouttopic` topic
- Processes one message at a time
- Iterates through all 5k event IDs in each message
- Coordinates data validation, retrieval, and insertion
- Implements recovery mechanism through offset tracking

### 4. **EventDataService** (`service/EventDataService.java`)
Database operations:
- Checks if event data exists in the database
- Inserts new event data
- Retrieves existing event data
- **NOTE**: Currently uses in-memory store for demonstration
- **TODO**: Replace with actual database implementation (JPA/Hibernate/MyBatis)

### 5. **EventRetrievalClient** (`client/EventRetrievalClient.java`)
API communication with retry logic:
- Calls external API: `GET /mspp/retrieve/eventid/{eventId}`
- Implements retry mechanism:
  - **Max retries**: 5 times
  - **Retry interval**: 1 second
  - **Exponential backoff**: Ready for implementation if needed
- Comprehensive error handling

### 6. **DTOs** (`dto/`)
- `EventMessage`: Represents parsed Kafka message
- `EventData`: Represents individual event with account number
- `RetrieveEventResponse`: API response structure

## Message Processing Flow

```
Kafka Message Received
        │
        ▼
┌─────────────────────────────────────────┐
│ Parse JSON Message                      │
│ serviceType: "wire"                     │
│ eventIds: ["id1|acc1", "id2|acc2", ...] │
└──────────────────┬──────────────────────┘
                   │
                   ▼
        ┌──────────────────────┐
        │ Parse Event ID Data  │
        │ Extract:             │
        │ - Event ID           │
        │ - Account Number     │
        └──────────────┬───────┘
                       │
         ┌─────────────┴─────────────┐
         │ For Each Event Record     │
         │ (Single-threaded loop)    │
         └──────────────┬────────────┘
                        │
                        ▼
         ┌──────────────────────────┐
         │ Check Database           │
         │ Data Exists?             │
         └──────────┬───────┬───────┘
                    │       │
              YES   │       │   NO
                    │       ▼
                    │  ┌────────────────────┐
                    │  │ Call External API  │
                    │  │ (with 5x retry)    │
                    │  │ 1 sec wait between │
                    │  └─────────┬──────────┘
                    │            │
                    │            ▼
                    │  ┌────────────────────┐
                    │  │ Insert to Database │
                    │  └────────────────────┘
                    │            │
                    └────────┬───┘
                             │
                             ▼
                    ┌────────────────────┐
                    │ All Records Done?  │
                    └─────┬──────────┬───┘
                          │          │
                       NO │          │ YES
                          │          ▼
                          │    ┌────────────────┐
                          │    │ Commit Offset  │
                          │    │ (Manual ACK)   │
                          │    └────────────────┘
                          │
                          └─────────┐
                                    ▼
                    ┌─────────────────────────┐
                    │ Continue with Next      │
                    │ Record or Message       │
                    └─────────────────────────┘
```

## Recovery Mechanism

The solution implements a robust recovery mechanism to handle failures:

### Offset Management
- **Partition**: Tracked for each message (`partition number`)
- **Offset**: Tracked for each message (`offset position`)
- **Manual Acknowledgment**: Only committed after successful processing
- **Recovery**: Failed messages automatically replay from the last committed offset

### Single-Record Failure Handling
If processing fails for a single record (e.g., record 2500 of 5000):
1. Exception is caught and logged
2. Processing continues with next record
3. Offset is committed only after all records complete
4. Failed record will be reprocessed if message is replayed

### Thread Failure Handling
If the thread fails completely:
1. The last committed offset remains unchanged
2. On restart, message consumption begins from the last acknowledged offset
3. Entire message is reprocessed from the beginning
4. Individual event records that succeeded are handled via idempotency

### Idempotency
The database check ensures idempotency:
```java
if (eventDataService.eventDataExists(eventId, accountNumber)) {
    // Skip - already processed
}
```

## Configuration

### application.properties
```properties
# Kafka Bootstrap
spring.kafka.bootstrap-servers=localhost:9092

# Consumer Settings
spring.kafka.consumer.group-id=fallout-event-consumer
spring.kafka.consumer.auto-offset-reset=earliest
spring.kafka.consumer.enable-auto-commit=false  # Manual commit

# Fallout Topic
kafka.fallout.topic.name=EM.fallouttopic

# Event API Configuration
event.api.base-url=http://localhost:8080

# Logging
logging.level.org.demo.project.dockersample=DEBUG
```

### KafkaConsumerConfig Settings
```java
// Single-threaded
factory.setConcurrency(1);

// One message at a time
MAX_POLL_RECORDS_CONFIG = 1

// Manual offset management
AckMode.MANUAL
```

## Error Handling

### Message-Level Errors
- Invalid JSON format: Message skipped, offset committed
- Missing eventIds: Message skipped, offset committed
- Empty message: Message skipped, offset committed

### Record-Level Errors
- Parse error on single record: Record skipped, processing continues
- API call fails after 5 retries: Record logged, error tracked
- Database insert fails: Exception logged, processing continues

### Thread-Level Errors
- Thread interruption: Gracefully handled, exception logged
- Unexpected runtime errors: Caught and logged, offset committed

## Retry Logic (API Calls)

```
Attempt 1
   │ (Failure)
   ▼
Wait 1 second
   │
Attempt 2
   │ (Failure)
   ▼
Wait 1 second
   │
Attempt 3
   │ (Failure)
   ▼
Wait 1 second
   │
Attempt 4
   │ (Failure)
   ▼
Wait 1 second
   │
Attempt 5
   │ (Failure)
   ▼
Throw Exception
```

## Database Implementation

### Current Implementation
- In-memory HashMap for demonstration
- Key format: `eventId_accountNumber`

### Required Implementation (TODO)
Replace with actual database queries:

```java
// Check existence
SELECT COUNT(*) FROM event_data 
WHERE event_id = ? AND account_number = ?

// Insert data
INSERT INTO event_data (event_id, account_number, event_data, status)
VALUES (?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
event_data = VALUES(event_data), status = VALUES(status)

// Retrieve data
SELECT * FROM event_data 
WHERE event_id = ? AND account_number = ?
```

### Database Schema (Suggested)
```sql
CREATE TABLE event_data (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    event_id VARCHAR(100) NOT NULL,
    account_number VARCHAR(100) NOT NULL,
    event_data TEXT,
    status VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_event_account (event_id, account_number),
    INDEX idx_event_id (event_id),
    INDEX idx_account_number (account_number)
);

CREATE TABLE event_processing_log (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    partition_id INT,
    offset_position BIGINT,
    total_records INT,
    processed_count INT,
    failed_count INT,
    status VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Logging

The solution provides comprehensive logging at different levels:

### DEBUG Level
- Message parsing details
- Individual record processing
- Database operation details

### INFO Level
- Message reception
- Successful insertions
- Offset acknowledgments

### WARN Level
- Partial failures
- Retry attempts
- Skipped records

### ERROR Level
- Parse failures
- API call failures
- Database operation failures

## Testing Considerations

### Test Message Format
```json
{
  "serviceType": "wire",
  "eventIds": [
    "event-001|ACC-12345",
    "event-002|ACC-12346",
    "event-003|ACC-12347"
  ]
}
```

### Test Scenarios
1. Valid message with all new records
2. Valid message with mixed (new and existing) records
3. API failure and retry recovery
4. Database failure handling
5. Thread interruption recovery
6. Message parsing errors
7. Large batch (5k records) processing

## Performance Considerations

### Single-Threaded Processing
- **Pros**: Simple recovery, predictable offset management, no concurrency issues
- **Cons**: Lower throughput, one message at a time

### Scaling Options (Future)
1. **Partitions**: Add more partitions to topic for parallel processing
2. **Consumer Groups**: Create multiple consumer instances with same group ID
3. **Batching**: Increase `MAX_POLL_RECORDS_CONFIG` for batch processing
4. **Thread Pool**: Implement thread pool for parallel event processing within a message

### Database Optimization
- Add indexes on `event_id` and `account_number`
- Use batch insert for better performance
- Implement connection pooling
- Consider caching for frequently accessed records

## Future Enhancements

1. **Metrics and Monitoring**
   - Add Micrometer metrics
   - Track processing time per message
   - Monitor API retry rates

2. **Distributed Tracing**
   - Integrate with Spring Cloud Sleuth
   - Add trace IDs to logs

3. **Circuit Breaker**
   - Implement Resilience4j circuit breaker for API calls
   - Fail fast on repeated failures

4. **Dead Letter Queue (DLQ)**
   - Send permanently failed messages to DLQ
   - Implement DLQ processing

5. **Batch Processing**
   - Implement batch database inserts
   - Batch API calls with deduplication

6. **State Management**
   - Store processing state in persistent store
   - Enable exact recovery without reprocessing

## Troubleshooting

### Consumer Not Receiving Messages
1. Check Kafka connectivity: `spring.kafka.bootstrap-servers`
2. Verify topic exists: `EM.fallouttopic`
3. Check consumer group ID in logs
4. Verify topic has messages

### Messages Not Being Processed
1. Check if consumer service is running
2. Review DEBUG logs for message parsing errors
3. Verify `@KafkaListener` annotation is present

### High Error Rates
1. Verify API endpoint is accessible
2. Check database connectivity
3. Review API response format
4. Monitor network latency

### Offset Not Advancing
1. Verify manual acknowledgment is enabled
2. Check for exceptions in processing
3. Monitor thread status
4. Review offset commit logs

## API Response Format

The external API should return a response compatible with `RetrieveEventResponse`:

```java
{
  "eventId": "event-001",
  "accountNumber": "ACC-12345",
  "eventData": "...",
  "status": "success"
}
```

## Security Considerations

1. **Authentication**: Add authentication to API calls
2. **TLS/SSL**: Use HTTPS for API calls
3. **Sensitive Data**: Encrypt sensitive information in logs
4. **Access Control**: Restrict database access
5. **Input Validation**: Validate all inputs thoroughly (already implemented)

## Support and Maintenance

- Monitor logs for patterns
- Track error metrics
- Regularly review and optimize queries
- Update dependencies regularly
- Test recovery procedures regularly

