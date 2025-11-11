package com.demo.docker.config

import org.springframework.test.context.TestContext
import org.springframework.test.context.TestExecutionListener
import org.springframework.test.context.support.AbstractTestExecutionListener

/**
 * Custom listener to force Spring test context initialization for Spock tests.
 * This listener explicitly prepares the test context before test methods run.
 */
class SpringBootTestContextListener extends AbstractTestExecutionListener {

    @Override
    int getOrder() {
        // Run before most other listeners
        return LOWEST_PRECEDENCE - 1000
    }

    @Override
    void beforeTestClass(TestContext testContext) throws Exception {
        println "\n[SpringBootTestContextListener] beforeTestClass triggered"
        try {
            // Prepare the test context - this initializes Spring beans
            testContext.getApplicationContext()
            println "✅ ApplicationContext prepared and beans initialized"
        } catch (Exception e) {
            println "❌ Error preparing test context: ${e.message}"
            e.printStackTrace()
        }
    }

    @Override
    void prepareTestInstance(TestContext testContext) throws Exception {
        println "[SpringBootTestContextListener] prepareTestInstance triggered"
        // This is where Spring does dependency injection for JUnit tests
        // For Spock, autowiring should happen via Spring's test support
    }

    @Override
    void beforeTestMethod(TestContext testContext) throws Exception {
        println "[SpringBootTestContextListener] beforeTestMethod triggered"
        def context = testContext.getApplicationContext()
        if (context != null) {
            println "✅ ApplicationContext available at method level"
        } else {
            println "❌ ApplicationContext is NULL at method level"
        }
    }
}

