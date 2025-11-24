Summary of Enhancements
I have successfully enhanced your code with comprehensive retry logic and failure management. Here's what was implemented:
1. Enhanced EventConsumerService (EventConsumerService.java)
   Changes:
   Added dependency injection for KafkaProducerService and ObjectMapper
   Implemented 3-stage failure handling:
   First Attempt: Process all records, collect failures
   Local Retries (2 attempts): Retry only failed records up to 2 times
   Async Retry: Failed records published to retry topic for extended retry logic
   Key Behavior:
   When a single message fails: it's collected in failedRecords list and processing continues to next record
   After first attempt, only failed records are retried (not successful ones)
   After 2 local retries, remaining failures are published to retry topic
   All records are acknowledged after handling (no reprocessing by Kafka)
<hr></hr>
2. Created RetryEventConsumerService (New File)
Purpose: Handles async retry logic with exponential backoff
Features:
Consumes from EM.retrytopic
Implements exponential backoff: min(2^retryAttempt * 100ms, 30000ms)
5-retry limit counter with per-record tracking via retryCount field
Conditional routing:
If retryCount < 5: Increment count and republish to retry topic
If retryCount >= 5: Publish to final fallout topic EM.fallout-final
<hr></hr>
3. Enhanced KafkaProducerService (KafkaProducerService.java)
New Methods:
sendToRetryTopic(): Publishes failed records to EM.retrytopic for retry processing
sendToFalloutTopic(): Publishes permanently failed records to EM.fallout-final after 5 retries exhausted
<hr></hr>
4. Enhanced KafkaTopicConfig (KafkaTopicConfig.java)
New Topics Created:
EM.retrytopic: For failed records awaiting retry (3 partitions, 1 replica)
EM.fallout-final: For permanently failed records after 5 retries (3 partitions, 1 replica)
<hr></hr>
Flow Diagram

Message from EM.fallouttopic
↓
Parse Message
↓
Process ALL records in batch
(collect failures in list)
↓
Retry FAILED records 2 times
↓
Still Failed? → YES → Publish to EM.retrytopic (retryCount=0)
↓ NO
Acknowledge offset
   
---

From EM.retrytopic (RetryEventConsumerService):
Process record
↓
Success? → YES → Acknowledge & Done
↓ NO
retryCount < 5? → YES → Increment, republish to EM.retrytopic
↓ NO
Publish to EM.fallout-final (retryCount=5)
Acknowledge & Done
<hr></hr>

Key Points
Aspect
Before
After
Failed Record Handling
Logs & continues
Collects & retries
Retry Attempts
None
2 local + 5 async (total 7)
Failure Tracking
No counter
retryCount incremented per attempt
Backoff Strategy
None
Exponential backoff (100ms to 30s)
Final Destination
Lost
EM.fallout-final topic
Per-Record Processing
All together
Failed ones isolated & retried
<hr></hr>
Usage
The implementation automatically:
✅ Processes messages one at a time (concurrency=1)
✅ Collects failed records during initial processing
✅ Retries failures independently without re-processing successes
✅ Publishes to retry topic after local attempts exhausted
✅ Applies exponential backoff for retry delays
✅ Publishes to fallout topic after 5 retries exhausted
✅ Maintains retry counter in EventData for tracking