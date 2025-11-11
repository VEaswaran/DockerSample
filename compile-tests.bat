@echo off
echo Compiling and running tests...
cd C:\projects\DemoProject\DockerSample
mvn clean test-compile
echo.
echo Compilation complete. To run tests, execute:
echo mvn test -Dtest=DockerComponentSpec
pause

