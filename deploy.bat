@echo off
echo ============================================
echo Docker Sample - Kafka Integration Deployment
echo ============================================
echo.

echo Step 1: Stopping any existing containers...
docker-compose down
echo.

echo Step 2: Pulling required images...
docker pull confluentinc/cp-zookeeper:7.5.0
docker pull confluentinc/cp-kafka:7.5.0
echo.

echo Step 3: Building and starting services...
echo This may take a few minutes on first run...
docker-compose up -d --build
echo.

echo Step 4: Waiting for services to start (45 seconds)...
echo Please wait while Zookeeper, Kafka, and the application initialize...
timeout /t 45 /nobreak
echo.

echo Step 5: Checking service status...
docker-compose ps
echo.

echo ============================================
echo Deployment Complete!
echo ============================================
echo.
echo Services running:
echo   - Zookeeper:    localhost:2181
echo   - Kafka:        localhost:9092
echo   - Application:  http://localhost:8080/docker/hello
echo.
echo Test the application:
echo   curl http://localhost:8080/docker/hello
echo.
echo View logs:
echo   docker-compose logs -f
echo.
echo Stop services:
echo   docker-compose down
echo.
echo Press any key to view application logs...
pause > nul

docker-compose logs -f docker-sample-app

