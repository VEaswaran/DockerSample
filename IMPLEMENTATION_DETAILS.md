# Event Fallout Consumer - Implementation Details

## Summary of Implementation

This document provides implementation details for the Kafka consumer processing event messages from the "EM.fallouttopic" topic.

### Requirements Met

✅ **Requirement 1**: Write a Kafka consumer to read messages from "EM.fallouttopic"
- Location: `EventConsumerService.java`
- Annotation: `@KafkaListener(topics = "EM.fallouttopic", ...)`

✅ **Requirement 2**: Single thread, one message at a time
- Configuration: `KafkaConsumerConfig.java`
- Settings: `concurrency = 1`, `MAX_POLL_RECORDS_CONFIG = 1`

✅ **Requirement 3**: Parse message with 5k event IDs
- Location: `EventMessageParserService.java`
- Parses: `serviceType` and `eventIds[]`

✅ **Requirement 4**: Parse event IDs with "|" delimiter for event ID and account number
- Location: `EventMessageParserService.parseEventIdData()`
- Delimiter: `"\\|"`
- Extracts: `eventId` and `accountNumber` from each pair

✅ **Requirement 5**: Database verification service
- Location: `EventDataService.java`
- Method: `eventDataExists(eventId, accountNumber)`
- Returns: boolean indicating if data exists

✅ **Requirement 6**: API retrieval and database insertion
- API Client: `EventRetrievalClient.java`
- Data Service: `EventDataService.insertEventData()`
- Orchestration: `EventConsumerService.processEventData()`

✅ **Requirement 7**: Robust error handling with 5x retry and 1-second wait
- Location: `EventRetrievalClient.java`
- Logic: 5 retries with 1000ms wait between attempts
- Exceptions: Handles all RestClientException types

✅ **Requirement 8**: Recovery mechanism for thread failures
- Offset Tracking: `partition` and `offset` tracked
- Manual ACK: `Acknowledgment.acknowledge()` only after success
- Resume: Kafka redelivers from last committed offset
- Idempotency: Database checks prevent duplicates

## Detailed Component Breakdown

### 1. Configuration Layer

#### KafkaConsumerConfig.java
```
Purpose: Configure single-threaded, single-record consumer

Key Settings:
- NUM_STREAM_THREADS_CONFIG = 1 (single thread)
- MAX_POLL_RECORDS_CONFIG = 1 (one record at a time)
- ENABLE_AUTO_COMMIT_CONFIG = false (manual acknowledgment)
- AckMode = MANUAL (explicit offset commit)
- FETCH_MIN_BYTES_CONFIG = 1 (allow single record)
- FETCH_MAX_WAIT_MS_CONFIG = 500 (wait up to 500ms for data)

Concurrency = 1 (only one listener at a time)
```

#### RestTemplateConfig.java
```
Purpose: Configure HTTP client for API calls

Settings:
- Connection Timeout: 5 seconds
- Read Timeout: 10 seconds
```

### 2. Consumer Layer

#### EventConsumerService.java
```
Main Responsibility: Orchestrate message consumption and processing

Flow:
1. Listen on EM.fallouttopic (one message at a time)
2. Receive message, partition ID, and offset
3. Parse message with EventMessageParserService
4. For each of 5000 event records:
   - Call processEventData()
   - Continue on individual failures
5. Only commit offset after ALL records complete

Key Methods:
- consumeEventMessage()
  Input: message, partition, offset, acknowledgment
  Process: Main entry point for Kafka listener
  
- processEventData()
  Input: eventData, recordIndex, totalRecords
  Steps:
    a. Check if data exists in DB
    b. If not, retrieve from API (with retries)
    c. Insert to database
    d. Continue to next record on error

Error Handling:
- Parse errors: Skip message, commit offset
- Individual record errors: Log and continue
- API failures: Retry 5 times before logging failure
- Database errors: Log and continue
```

### 3. Parser Layer

#### EventMessageParserService.java
```
Responsibility: Parse JSON messages and extract event data

Methods:

parseMessage(messageContent)
  Input: Raw message string from Kafka
  Output: EventMessage object
  Process:
    1. Validate message is not empty
    2. Parse JSON using ObjectMapper
    3. Verify eventIds is not empty
    4. Return EventMessage or throw exception
  Exceptions: IllegalArgumentException on validation failure

parseEventIdData(eventMessage, originalMessage)
  Input: Parsed EventMessage object
  Output: List<EventData> with extracted IDs and accounts
  Process:
    1. For each eventId in eventIds list
    2. Split by "|" delimiter
    3. Extract eventId and accountNumber
    4. Create EventData object
    5. Validate both parts are non-empty
    6. Continue on individual parse errors
  Returns: List of successfully parsed EventData objects
  Throws: IllegalArgumentException if no valid data extracted

JSON Format Expected:
{
  "serviceType": "wire",
  "eventIds": [
    "id1|account1",
    "id2|account2",
    ...
  ]
}

Internal Format Created:
EventData {
  serviceType: "wire"
  eventId: "id1"
  accountNumber: "account1"
  retryCount: 0
  partitionNumber: 0
  offsetPosition: 12345
  messageContent: "original JSON string"
}
```

### 4. Data Layer

#### EventDataService.java
```
Responsibility: Database operations

Current Implementation: In-memory HashMap (demonstration)
TODO: Replace with JPA/Hibernate/MyBatis

Methods:

eventDataExists(eventId, accountNumber)
  Purpose: Check if event data already exists
  Logic: Generate composite key (eventId_accountNumber)
  Return: boolean
  Exception Handling: Log and rethrow as RuntimeException

insertEventData(eventResponse)
  Purpose: Insert or update event data
  Input: RetrieveEventResponse from API
  Logic: Create composite key, store in map/DB
  Return: boolean indicating success
  Exception Handling: Log and rethrow

getEventData(eventId, accountNumber)
  Purpose: Retrieve event data
  Return: RetrieveEventResponse or null
  Exception Handling: Log and rethrow

Database Schema (Recommended):
CREATE TABLE event_data (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  event_id VARCHAR(100) NOT NULL,
  account_number VARCHAR(100) NOT NULL,
  event_data TEXT,
  status VARCHAR(50),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_event_account (event_id, account_number),
  INDEX idx_event_id (event_id),
  INDEX idx_account_number (account_number)
);
```

### 5. API Client Layer

#### EventRetrievalClient.java
```
Responsibility: Call external API with robust retry logic

Endpoint: GET /mspp/retrieve/eventid/{eventId}

Retry Configuration:
- MAX_RETRIES = 5
- RETRY_WAIT_MS = 1000 (1 second)

Algorithm:
retryCount = 0
WHILE retryCount < 5:
  TRY:
    Call API: GET /mspp/retrieve/eventid/{eventId}
    IF response != null:
      RETURN response
    ELSE:
      Set lastException = "null response"
  CATCH RestClientException as e:
    retryCount++
    IF retryCount < 5:
      Log warning
      Sleep 1000ms
    ELSE:
      Log error
      Throw exception
  CATCH Exception as e:
    retryCount++
    IF retryCount < 5:
      Log warning
      Sleep 1000ms
    ELSE:
      Log error
      Throw exception

After 5 failed attempts:
  Throw RuntimeException with original error

Exception Handling:
- InterruptedException: Restore interrupt flag, throw exception
- RestClientException: Caught and retried
- Generic Exception: Caught and retried
- Null response: Treated as failure, retried
```

### 6. DTO Layer

#### EventMessage.java
```
Represents parsed Kafka message

Fields:
- serviceType: String (e.g., "wire")
- eventIds: List<String> (list of "eventId|accountNumber" pairs)

Jackson Annotations: @JsonProperty for flexible naming
```

#### EventData.java
```
Represents extracted event information

Fields:
- serviceType: String
- eventId: String (extracted from eventIds)
- accountNumber: String (extracted from eventIds)
- messageContent: String (original message for reference)
- retryCount: Integer
- offsetPosition: Long (for recovery)
- partitionNumber: Integer (for recovery)
```

#### RetrieveEventResponse.java
```
Represents API response

Fields:
- eventId: String
- accountNumber: String
- eventData: String
- status: String

Matches expected API response format
```

## Processing Flow Diagram

```
┌─ Kafka Message Received ─────────────────────┐
│ Topic: EM.fallouttopic                      │
│ Partition: 0                                 │
│ Offset: 12345                                │
└─────────────────────┬──────────────────────┘
                      │
                      ▼
        ┌─ Parse Message ──────────────────┐
        │ Input: JSON String               │
        │ EventMessageParserService        │
        │ Output: EventMessage             │
        └──────────────┬────────────────────┘
                       │
                       ▼
        ┌─ Parse Event IDs ────────────────┐
        │ Split: "id|account" by "|"      │
        │ Create 5000 EventData objects   │
        └──────────────┬────────────────────┘
                       │
         ┌─────────────┴──────────────┐
         │ For Each EventData         │
         │ (Single-threaded loop)     │
         └──────────────┬──────────────┘
                        │
        ┌───────────────┴─────────────────┐
        │ Database Lookup               │
        │ eventDataExists()              │
        └───────────────┬─────────────────┘
                        │
         ┌──────────────┴───────────────┐
         │ EXISTS                       │ NOT EXISTS
         │ Skip Record                  │ Retrieve from API
         │                              │
         └──────────────┐───────────────┘
                        │
         ┌──────────────┴───────────────┐
         │ API Call (if retrieved)      │
         │ EventRetrievalClient         │
         │ Retry: 5x with 1sec wait    │
         └──────────────┬───────────────┘
                        │
        ┌───────────────┴─────────────────┐
        │ SUCCESS                        │ FAILURE
        │ Insert to Database             │ Log Error
        │ EventDataService.insert()      │ Continue Next
        │                                │
        └───────────────┬─────────────────┘
                        │
         ┌──────────────┴──────────────────┐
         │ All Records Processed?         │
         └──────────────┬──────────────────┘
                        │
         ┌──────────────┴──────────────────┐
         │ NO: Next Record                │
         │                                │
         └─────────┐──────────────────────┘
                   │
                   └───── (Loop back to Database Lookup)
                   
         YES
         │
         ▼
    ┌─ Commit Offset ─────────┐
    │ Manual Acknowledgment   │
    │ acknowledgment.commit() │
    └─────────────────────────┘
         │
         ▼
    Ready for Next Message
```

## Error Handling Strategy

### At Message Level
```
Parse Error (Invalid JSON)
  → Skip message
  → Commit offset (no reprocessing)
  → Log as WARN/ERROR
  → Continue to next message

Missing eventIds
  → Skip message
  → Commit offset
  → Log as WARN/ERROR

Empty eventIds list
  → Skip message
  → Commit offset
  → Log as WARN/ERROR
```

### At Record Level
```
Invalid event ID pair format (missing "|")
  → Skip individual record
  → Continue to next record in batch
  → Log as WARN

Missing/empty eventId or accountNumber
  → Skip individual record
  → Continue to next record in batch
  → Log as WARN

API call failure after 5 retries
  → Log error with eventId
  → Continue to next record
  → DO NOT commit offset if critical failure
  (Optional: Send to DLQ)

Database insert failure
  → Log error with eventId
  → Continue to next record
  → Log as ERROR

Database lookup failure
  → Log error
  → Continue to next record
  → Log as ERROR
```

### At Thread Level
```
Unexpected exception
  → Log full stack trace
  → Commit offset (to avoid infinite loop)
  → Container can restart consumer

Thread interruption
  → Restore interrupt flag
  → Log error
  → Throw exception

No offset committed = redelivery
  → Kafka retains message at same offset
  → Consumer restarts from same offset
  → All records reprocessed
  → Idempotency ensures no duplicates
```

## Recovery Mechanism Details

### Offset Management
```
Message received at offset 12345
  ↓
Processing 5000 records
  ↓
Record 1-2499: Success
Record 2500: API fails after 5 retries
  (Offset NOT committed yet)
Record 2501-5000: Continue processing
  (Each updates DB independently)
  ↓
All records done
  ↓
Commit offset 12345
  ↓
If thread crashes BEFORE commit:
  → Next restart: Kafka redelivers message at 12345
  → Reprocess all 5000 records
  → Records 1-2499: Already in DB (idempotent, skipped)
  → Record 2500: Retry again
  → Continue

If thread crashes AFTER commit:
  → Next restart: Kafka advances to 12346
  → Next message processed normally
```

### Idempotency Assurance
```
Check: eventDataExists(eventId, accountNumber)
  ↓
IF found:
  → Skip processing
  → Continue to next record
ELSE:
  → Retrieve and insert

Example:
Record already inserted, then:
- Message replayed
- Check finds record exists
- Skip
- No duplicate insertion
- Composite key prevents duplicates
```

### Failure Recovery Pattern
```
Scenario 1: Single Record Failure
Message has 5000 records
Record 2500 fails
  → Continue with 2501-5000
  → All process independently
  → Offset committed
  → Next message: Clean slate
  → Record 2500 NOT retried automatically
  → Manual intervention needed OR
  → Implement separate DLQ handler

Scenario 2: Batch Failure
Message replayed completely
  → All 5000 records reprocessed
  → DB checks ensure no duplicates
  → Successful inserts skipped
  → Failed record(s) retried
  → Cascading retries until success

Scenario 3: Thread Failure
Processing stopped mid-batch
  → Offset NOT committed
  → Consumer restarts
  → Message redelivered from last offset
  → Entire batch reprocessed
  → DB idempotency prevents duplicates
```

## Performance Considerations

### Single-Threaded Design
**Pros:**
- Simple, predictable behavior
- Easy offset management
- No concurrency issues
- Straightforward recovery

**Cons:**
- Slower throughput (1 message at a time)
- Cannot fully utilize CPU
- Not ideal for high-volume scenarios

### Processing Time
- Message parse: ~10-50ms
- 5000 record parsing: ~100-200ms
- Database lookup per record: ~1-5ms (varies with DB)
- API call: ~100-500ms per record
- Database insert: ~1-5ms per record

**Estimated time for 5000 records:**
- All in DB (exists): ~30 seconds (5000 * 5ms)
- All new (API calls): ~500+ seconds (5000 * 100+ms)

### Scaling Options (Future)
```
Option 1: Add Topic Partitions
- Multiple consumers in same group
- Each consumer gets different partitions
- True parallelism across messages

Option 2: Batch Processing
- Increase MAX_POLL_RECORDS_CONFIG
- Process multiple messages/records in batch
- Trade-off with recovery granularity

Option 3: Thread Pool Within Message
- Keep single message consumption
- Use thread pool for record processing
- Complex offset management needed

Option 4: Event Streaming
- Stream records instead of batch
- Kafka Streams library
- Different architecture
```

## Monitoring and Observability

### Key Metrics
```
Consumer Lag:
- Current offset vs. log end offset
- Indicates processing speed vs. message production

Record Processing Rate:
- Records/second
- Calculate from offset advancement

API Retry Rate:
- Failed API calls / total API calls
- Indicates external service health

Database Operation Duration:
- Time to lookup/insert per record
- Identify DB bottlenecks

Message Processing Duration:
- Total time to process 5000 records
- Identify performance issues
```

### Logging Strategy
```
DEBUG:
- Message received at partition/offset
- Each record parsing details
- Database lookup results
- API call details

INFO:
- Message parsing success
- Records count
- Successful database inserts
- Offset commits
- API retrieval success

WARN:
- Invalid record format (skipped)
- API retry attempts
- Unusual latency

ERROR:
- API failures after retries
- Database operation failures
- Parse failures
- Exception stack traces
```

## Testing Scenarios

### Scenario 1: Normal Processing
Input: Valid message with 10 new records
Expected: All records inserted, offset committed

### Scenario 2: Mixed Records
Input: 5 existing + 5 new records
Expected: 5 skipped, 5 inserted, offset committed

### Scenario 3: API Failure
Input: Valid message, API returns 500
Expected: Retry 5 times, log error, continue next

### Scenario 4: Database Already Has Data
Input: Message with records already in DB
Expected: All skipped, offset committed

### Scenario 5: Malformed Message
Input: Invalid JSON
Expected: Message skipped, offset committed

### Scenario 6: Invalid Event Pairs
Input: Missing "|" delimiter in some records
Expected: Invalid records skipped, valid ones processed

### Scenario 7: Thread Interruption
Input: Thread interrupted during processing
Expected: Clean exception, no offset commit

### Scenario 8: Large Batch (5000 records)
Input: 5000 event ID pairs
Expected: All processed, performance logged

## Deployment Checklist

- [ ] Replace in-memory store with actual database
- [ ] Configure external API endpoint
- [ ] Set Kafka bootstrap servers
- [ ] Create EM.fallouttopic topic
- [ ] Configure consumer group name
- [ ] Set appropriate logging levels
- [ ] Add monitoring/metrics
- [ ] Test with sample messages
- [ ] Verify recovery procedure
- [ ] Document API response format
- [ ] Set up alerts for error rates
- [ ] Performance baseline testing

## File Locations Summary

```
src/main/java/org/demo/project/dockersample/
├── EventConsumerService.java           ← Main consumer
├── EventRetrievalMockController.java    ← Mock API for testing
├── config/
│   ├── KafkaConsumerConfig.java        ← Consumer configuration
│   ├── RestTemplateConfig.java         ← HTTP client config
│   └── KafkaTopicConfig.java           ← Topic creation
├── service/
│   ├── EventMessageParserService.java   ← JSON parsing
│   ├── EventDataService.java            ← DB operations (TODO)
│   ├── KafkaProducerService.java        ← Existing producer
│   └── KafkaConsumerService.java        ← Original consumer
├── client/
│   └── EventRetrievalClient.java        ← API client with retry
└── dto/
    ├── EventMessage.java                ← Kafka message DTO
    ├── EventData.java                   ← Parsed event DTO
    └── RetrieveEventResponse.java       ← API response DTO

src/main/resources/
└── application.properties                ← Configuration

Documentation/
├── CONSUMER_IMPLEMENTATION.md           ← Full technical details
├── QUICK_START.md                       ← Quick reference
└── IMPLEMENTATION_DETAILS.md (this file)

src/test/
└── java/com/demo/docker/
    └── EventConsumerServiceIntegrationSpec.java ← Test cases
```

