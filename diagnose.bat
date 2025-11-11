@echo off
echo ============================================
echo Kafka Troubleshooting and Diagnostics
echo ============================================
echo.

echo Checking Docker status...
docker --version
echo.

echo Checking running containers...
docker ps
echo.

echo Checking all containers (including stopped)...
docker ps -a
echo.

echo ============================================
echo Zookeeper Status
echo ============================================
docker logs zookeeper --tail 50
echo.

echo ============================================
echo Kafka Status
echo ============================================
docker logs kafka --tail 50
echo.

echo ============================================
echo Application Status
echo ============================================
docker logs docker-sample-container --tail 50
echo.

echo ============================================
echo Network Status
echo ============================================
docker network ls
echo.

echo ============================================
echo Volume Status
echo ============================================
docker volume ls
echo.

echo Press any key to exit...
pause > nul

