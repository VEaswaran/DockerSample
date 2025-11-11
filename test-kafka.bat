@echo off
echo ======================================
echo Testing Kafka Integration
echo ======================================
echo.

echo Test 1: GET /docker/hello (publishes "Hello World" to Kafka)
curl http://localhost:8080/docker/hello
echo.
echo.

echo Test 2: POST /docker/publish with custom message
curl -X POST "http://localhost:8080/docker/publish?message=Testing Kafka Integration"
echo.
echo.

echo Test 3: POST /docker/publish-with-key with key and message
curl -X POST "http://localhost:8080/docker/publish-with-key?key=test-key-1&message=Message with Key"
echo.
echo.

echo ======================================
echo Verifying messages in Kafka...
echo ======================================
echo.
echo Consuming messages from topic com.docker.msg.test:
echo (Press Ctrl+C to exit)
echo.

docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic com.docker.msg.test --from-beginning

