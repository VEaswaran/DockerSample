package com.demo.docker.config

import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Configuration

/**
 * Test configuration to ensure Spring context is properly loaded in Spock tests.
 * This is required for @SpringBootTest integration with Spock.
 */
@Configuration
@TestConfiguration
class SpockTestConfig {
    // Empty config - just ensures Spring context can be properly initialized
}

