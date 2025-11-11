@echo off
echo ============================================
echo Complete Cleanup - DockerSample
echo ============================================
echo.
echo WARNING: This will remove all containers, volumes, and data!
echo.
set /p confirm="Are you sure you want to continue? (yes/no): "

if /i not "%confirm%"=="yes" (
    echo Cleanup cancelled.
    exit /b
)

echo.
echo Step 1: Stopping all containers...
docker-compose down
echo.

echo Step 2: Removing all containers...
docker-compose down -v
echo.

echo Step 3: Removing Docker images...
docker rmi docker-sample:latest 2>nul
echo.

echo Step 4: Pruning unused Docker resources...
docker system prune -f
echo.

echo Step 5: Listing remaining containers...
docker ps -a
echo.

echo Step 6: Listing remaining volumes...
docker volume ls
echo.

echo ============================================
echo Cleanup Complete!
echo ============================================
echo.
echo You can now run deploy.bat to start fresh.
echo.
pause

