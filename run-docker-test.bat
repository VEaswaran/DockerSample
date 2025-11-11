@echo off
echo ========================================
echo Running Docker Component Spec Test
echo ========================================
echo.
cd C:\projects\DemoProject\DockerSample
echo Cleaning and running test...
echo.
mvn clean test -Dtest=DockerComponentSpec
echo.
echo ========================================
echo Test execution complete
echo ========================================
pause

