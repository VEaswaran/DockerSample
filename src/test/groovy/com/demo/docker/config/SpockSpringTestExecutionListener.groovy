package com.demo.docker.config

import org.springframework.test.context.TestContext
import org.springframework.test.context.TestExecutionListener
import org.springframework.test.context.support.AbstractTestExecutionListener

/**
 * Custom TestExecutionListener to ensure Spring context is properly initialized for Spock tests.
 * This listener explicitly hooks into Spring's test context lifecycle.
 */
class SpockSpringTestExecutionListener extends AbstractTestExecutionListener {

    @Override
    int getOrder() {
        return LOWEST_PRECEDENCE
    }

    @Override
    void beforeTestMethod(TestContext testContext) throws Exception {
        println "✅ TestExecutionListener: Before test method - Spring context is ready"
        println "   Application context: ${testContext.getApplicationContext()}"
    }

    @Override
    void afterTestMethod(TestContext testContext) throws Exception {
        println "✅ TestExecutionListener: After test method"
    }
}

