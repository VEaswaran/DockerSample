# Event Fallout Consumer - Quick Start Guide

## Overview

This guide explains how to use the Event Fallout Consumer implementation.

## Key Features

✅ **Single-threaded processing** - One message at a time  
✅ **One record at a time** - Processes single event ID+account number pair  
✅ **Database validation** - Checks if data exists before API call  
✅ **API retry logic** - 5 retries with 1-second wait between attempts  
✅ **Error resilience** - Continues on individual record failures  
✅ **Recovery mechanism** - Resumes from last known position on thread failure  
✅ **Manual offset management** - Full control over checkpoint positions  

## Project Structure

```
src/main/java/org/demo/project/dockersample/
├── config/
│   ├── KafkaConsumerConfig.java      # Consumer configuration (1 thread, 1 record)
│   ├── KafkaTopicConfig.java         # Topic creation
│   └── RestTemplateConfig.java       # HTTP client configuration
├── service/
│   ├── EventConsumerService.java      # Main consumer (listens to EM.fallouttopic)
│   ├── EventMessageParserService.java # JSON parsing and event ID extraction
│   ├── EventDataService.java          # Database operations (TODO: implement)
│   └── KafkaProducerService.java      # Existing producer (unchanged)
├── client/
│   └── EventRetrievalClient.java     # API calls with 5x retry logic
└── dto/
    ├── EventMessage.java             # Kafka message structure
    ├── EventData.java                # Parsed event information
    └── RetrieveEventResponse.java    # API response structure
```

## Message Format

### Input Message (from Kafka topic "EM.fallouttopic")
```json
{
  "serviceType": "wire",
  "eventIds": [
    "1133-43345-5w4-3344|accountId_1",
    "1133-43345-5w4-3344|accountId_2",
    "1133-43345-5w4-3344|accountId_3",
    "..."  // up to 5000 records
  ]
}
```

### Parsed Format (Internal)
```java
EventData {
  serviceType: "wire",
  eventId: "1133-43345-5w4-3344",
  accountNumber: "accountId_1",
  retryCount: 0,
  partitionNumber: 0,
  offsetPosition: 12345
}
```

### API Response Expected
```json
{
  "eventId": "1133-43345-5w4-3344",
  "accountNumber": "accountId_1",
  "eventData": "...",
  "status": "success"
}
```

## Processing Flow

### For Each Message:

1. **Parse JSON Message**
   - Extract serviceType
   - Extract eventIds list

2. **For Each Event ID in eventIds (5000 items)**
   - Parse "eventId|accountNumber" format
   - Create EventData object

3. **For Each EventData**
   - Check: Does data exist in database?
     - **YES**: Skip to next record
     - **NO**: Call API to retrieve
   
4. **On API Call (if needed)**
   - Attempt 1: Call API
   - If fails, wait 1 second, retry
   - Repeat up to 5 times
   - If all fail: Log error, move to next record

5. **On Successful API Response**
   - Insert data into database
   - Log success

6. **After All Records Processed**
   - Commit offset to Kafka
   - Ready for next message

## Configuration

### application.properties
```properties
# Kafka Consumer - Auto-configured by KafkaConsumerConfig
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.consumer.group-id=fallout-event-consumer

# Fallout Topic
kafka.fallout.topic.name=EM.fallouttopic

# API Endpoint
event.api.base-url=http://localhost:8080

# Logging
logging.level.org.demo.project.dockersample=DEBUG
```

### Consumer Settings (in KafkaConsumerConfig)
```java
// Single-threaded processing
concurrency = 1

// Process one message at a time
MAX_POLL_RECORDS_CONFIG = 1

// Manual offset management (for recovery)
AckMode.MANUAL
```

## Retry Logic Details

```
API Call Failed
    ↓
Wait 1 second
    ↓
Retry 2 (attempt 2 of 5)
    ↓
Timeout or Connection Error
    ↓
Wait 1 second
    ↓
Retry 3 (attempt 3 of 5)
    ↓
... (repeats up to attempt 5)
    ↓
If all fail: Throw exception
    ↓
Continue with next record
```

**Exceptions caught:**
- `RestClientException` - Network issues
- Timeout exceptions
- Generic exceptions

## Recovery Mechanism

### Scenario 1: Single Record Fails
```
Processing record 2500 of 5000
API call fails 5 times
Log error
Continue with record 2501
After all records → Commit offset
Next message: New message processed
```
Result: Failed record stays in database (not updated), continues.

### Scenario 2: Thread Crashes at Record 2500
```
Processing record 2500 of 5000
Thread crashes
Offset NOT committed
Consumer restarts
Kafka redelivers same message from offset
Start processing from record 1 again
Record 1-2499 already in DB (idempotent)
Record 2500+ processes normally
After all records → Commit offset
```
Result: Message reprocessed, duplicates handled by DB check.

### Scenario 3: Database Insert Fails
```
API succeeds, data retrieved
Insert to DB fails
Exception logged
Continue with next record
Offset IS committed
Next attempt: Record skipped (already in DB is false)
```
Result: Handled by retry/manual intervention.

## Database Implementation (TODO)

### Current State
- In-memory HashMap (demonstration only)
- Located in `EventDataService.eventDataStore`

### Required Implementation
Replace with actual database using JPA:

```java
@Repository
public interface EventDataRepository extends JpaRepository<EventDataEntity, Long> {
    
    @Query("SELECT COUNT(*) > 0 FROM EventData e " +
           "WHERE e.eventId = :eventId AND e.accountNumber = :accountNumber")
    boolean exists(String eventId, String accountNumber);
    
    @Query("SELECT e FROM EventData e " +
           "WHERE e.eventId = :eventId AND e.accountNumber = :accountNumber")
    Optional<EventDataEntity> findByEventIdAndAccountNumber(String eventId, String accountNumber);
}
```

### Entity Definition
```java
@Entity
@Table(name = "event_data", uniqueConstraints = {
    @UniqueConstraint(columnNames = {"event_id", "account_number"})
})
public class EventDataEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(name = "event_id", nullable = false)
    private String eventId;
    
    @Column(name = "account_number", nullable = false)
    private String accountNumber;
    
    @Column(name = "event_data", columnDefinition = "LONGTEXT")
    private String eventData;
    
    @Column(name = "status")
    private String status;
    
    @CreationTimestamp
    private LocalDateTime createdAt;
    
    @UpdateTimestamp
    private LocalDateTime updatedAt;
}
```

## Testing

### Test Message (Small)
```bash
# Publish to EM.fallouttopic
{
  "serviceType": "wire",
  "eventIds": [
    "TEST-001|ACC-001",
    "TEST-002|ACC-002",
    "TEST-003|ACC-003"
  ]
}
```

### Expected Behavior
1. Message received and logged
2. 3 event IDs parsed
3. For each:
   - Check DB (not found)
   - Call API: `GET /mspp/retrieve/eventid/TEST-001`
   - Insert to DB
4. Offset committed
5. Ready for next message

### Logging Output (DEBUG)
```
Received message from partition: 0, offset: 100
Parsed event message with serviceType: wire, eventIdCount: 3
Successfully parsed 3 event data entries from message
Processing record 1/3: eventId=TEST-001, accountNumber=ACC-001
Data not found in database for eventId: TEST-001, accountNumber: ACC-001 - RETRIEVING from API
Successfully retrieved event data for eventId: TEST-001
Successfully inserted event data for eventId: TEST-001, accountNumber: ACC-001
Successfully acknowledged offset: 100 for partition: 0
```

## Monitoring

### Key Metrics to Track
- Messages processed per minute
- Average processing time per message
- Average records per message
- Failed records count
- API retry rate
- Database operation success rate
- Consumer lag

### Log Patterns to Monitor
```
WARN: "API call failed for eventId: X, retrying"  → Check API health
ERROR: "Failed to retrieve event data for eventId: X after 5 retries"  → API issues
ERROR: "Error inserting event data"  → DB issues
INFO: "Successfully acknowledged offset"  → Normal operation
```

## Troubleshooting

### Issue: Messages not being consumed
**Solutions:**
1. Verify Kafka is running: `localhost:9092`
2. Verify topic exists: `EM.fallouttopic`
3. Check logs: `grep "Received message" application.log`
4. Verify consumer group: `fallout-event-consumer`

### Issue: High error rate on API calls
**Solutions:**
1. Check API endpoint: `http://localhost:8080/mspp/retrieve/eventid/{eventId}`
2. Monitor network latency
3. Increase timeout in RestTemplateConfig
4. Add circuit breaker (Resilience4j)

### Issue: Consumer appears stuck
**Solutions:**
1. Check thread status
2. Review logs for exceptions
3. Check database connectivity
4. Monitor memory usage

### Issue: Offset not advancing
**Solutions:**
1. Check manual acknowledgment is working
2. Review exception logs
3. Verify database insert is succeeding
4. Check offset commit logs

## Next Steps

1. ✅ **Implement Database Service**
   - Replace in-memory HashMap with JPA repository
   - Create entity and repository classes
   - Add transaction management

2. ✅ **Configure External API**
   - Update `event.api.base-url` in properties
   - Test API connectivity
   - Verify response format matches `RetrieveEventResponse`

3. ✅ **Set up Monitoring**
   - Add Micrometer metrics
   - Configure log aggregation (ELK, Splunk)
   - Set up alerts for error rates

4. ✅ **Performance Optimization**
   - Add database indexes
   - Implement batch operations
   - Consider connection pooling

5. ✅ **Production Deployment**
   - Add health checks
   - Configure proper JVM settings
   - Set up backup offset tracking
   - Implement graceful shutdown

## Contact & Support

For questions or issues:
1. Check logs: `logging.level.org.demo.project.dockersample=DEBUG`
2. Review CONSUMER_IMPLEMENTATION.md for detailed documentation
3. Check test cases in `EventConsumerServiceIntegrationSpec`

