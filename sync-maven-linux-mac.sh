#!/bin/bash
# Maven Dependency Sync Script for Linux/Mac

cd "$(dirname "$0")"

echo ""
echo "=========================================="
echo "Maven Dependency Sync - Linux/Mac"
echo "=========================================="
echo ""
echo "Downloading dependencies..."
echo ""

# Clean Maven cache and download dependencies
mvn clean install -DskipTests -q

if [ $? -eq 0 ]; then
    echo ""
    echo "=========================================="
    echo "SUCCESS! All dependencies downloaded"
    echo "=========================================="
    echo ""
    echo "Please reload your project in your IDE:"
    echo "- IntelliJ: Right-click pom.xml > Maven > Reload project"
    echo "- VS Code: Press Ctrl+Shift+O"
    echo "- Command: mvn clean install -DskipTests"
    echo ""
else
    echo ""
    echo "=========================================="
    echo "ERROR! Maven download failed"
    echo "=========================================="
    echo ""
    echo "Try these steps:"
    echo "1. Ensure Maven is installed: mvn -version"
    echo "2. Check internet connection"
    echo "3. Run: mvn clean install -X -DskipTests"
    echo "4. Check for firewall/proxy issues"
    echo ""
fi

