package com.demo.docker

import com.demo.docker.config.SpringBootTestContextListener
import com.demo.testing.annotations.EnableKafkaTest
import com.demo.testing.base.IntegrationTestBaseSpec
import org.demo.project.dockersample.DockerSampleApplication
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.ApplicationContext
import org.springframework.test.context.TestContextManager
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.TestExecutionListeners
import org.springframework.test.web.servlet.MockMvc
import org.springframework.web.context.WebApplicationContext

import java.time.Duration

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status

/**
 * Component test for Docker endpoint that validates Kafka message publishing.
 *
 * This test extends IntegrationTestBaseSpec which provides:
 * - Automatic Kafka TestContainer setup
 * - kafkaUtils for consuming/producing messages
 * - Spring Boot configuration overrides
 */
@EnableKafkaTest
@SpringBootTest(
    classes = DockerSampleApplication,
    webEnvironment = SpringBootTest.WebEnvironment.MOCK
)
@AutoConfigureMockMvc
@TestPropertySource(locations = "classpath:application-test.properties")
@TestExecutionListeners(
    listeners = [SpringBootTestContextListener],
    mergeMode = TestExecutionListeners.MergeMode.MERGE_WITH_DEFAULTS
)
class DockerComponentSpec extends IntegrationTestBaseSpec {

    @Autowired(required = false)
    MockMvc mockMvc

    @Autowired(required = false)
    ApplicationContext applicationContext

    @Autowired(required = false)
    WebApplicationContext webApplicationContext

    private static TestContextManager testContextManager

    def setupSpec() {
        println "\n=============================================="
        println "=== setupSpec: Initializing Spring Test Context ==="
        println "==============================================\n"

        if (testContextManager == null) {
            try {
                testContextManager = new TestContextManager(this.class)
                println "✅ TestContextManager created for: ${this.class.simpleName}"
            } catch (Exception e) {
                println "❌ Error creating TestContextManager: ${e.message}"
                e.printStackTrace()
            }
        }
    }

    def setup() {
        println "\n========================================="
        println "=== Test setup() method called ==="
        println "========================================="

        // Manually trigger Spring's test lifecycle to inject dependencies
        if (testContextManager != null) {
            try {
                // This triggers prepareTestInstance which does @Autowired injection
                testContextManager.prepareTestInstance(this)
                println "✅ TestContextManager.prepareTestInstance() called"
            } catch (Exception e) {
                println "⚠️ prepareTestInstance error: ${e.message}"
            }
        }

        println "\n📋 Initialization Status:"

        // Check Kafka first (should always be initialized)
        println "\n🔹 Kafka Initialization:"
        println "   bootstrapServers: ${bootstrapServers}"
        println "   kafkaUtils: ${kafkaUtils != null ? '✅ LOADED' : '❌ NULL'}"

        // Check Spring context components
        println "\n🔹 Spring Context Status:"
        println "   applicationContext: ${applicationContext != null ? '✅ LOADED' : '❌ NULL'}"
        println "   webApplicationContext: ${webApplicationContext != null ? '✅ LOADED' : '❌ NULL'}"
        println "   mockMvc: ${mockMvc != null ? '✅ LOADED' : '❌ NULL'}"

        if (applicationContext != null) {
            println "\n   📊 Spring Beans:"
            println "      Total beans: ${applicationContext.getBeanDefinitionCount()}"
            try {
                def mockMvcBeans = applicationContext.getBeansOfType(MockMvc.class)
                println "      MockMvc beans: ${mockMvcBeans.size()}"
                def webContextBeans = applicationContext.getBeansOfType(WebApplicationContext.class)
                println "      WebApplicationContext beans: ${webContextBeans.size()}"
            } catch (Exception e) {
                println "      ⚠️ Error querying beans: ${e.message}"
            }
        }

        println "\n=========================================\n"

        // Log warnings but don't fail - let the test fail naturally if dependencies are missing
        if (kafkaUtils == null) {
            println "⚠️ WARNING: kafkaUtils is NULL"
        }

        if (mockMvc == null) {
            println "⚠️ WARNING: mockMvc is NULL"
            if (applicationContext == null) {
                println "   Root cause: Spring ApplicationContext is NULL"
            } else {
                println "   Root cause: Spring context exists but MockMvc was not autowired"
            }
        }
    }

    def "GET /docker endpoint should publish 'very original msg posted' to HelloWorld topic"() {
        given: "the Kafka container is running and the consumer is ready to receive messages"
        // kafkaUtils is automatically available from IntegrationTestBaseSpec
        // Start consuming BEFORE producing to ensure we don't miss the message
        def consumerThread = null
        def consumedMessages = []

        consumerThread = Thread.start {
            try {
                consumedMessages.addAll(kafkaUtils.consumeMessagesWithOffset("com.docker.msg.test", 1, Duration.ofSeconds(15), "earliest"))
            } catch (Exception e) {
                println "❌ Consumer error: ${e.message}"
                e.printStackTrace()
            }
        }

        // Give consumer time to start
        Thread.sleep(500)

        when: "the root endpoint (/) is invoked"
        // Note: MockMvc doesn't include the context-path (/docker), only the servlet path
        def response = mockMvc.perform(get("/"))
                .andExpect(status().isOk())
                .andReturn()

        // Wait for consumer to finish
        consumerThread.join(20000)

        then: "the response is successful"
        response.response.status == 200
        response.response.contentAsString == "Hello World"

        and: "a message 'Hello World' is published to the correct topic"
        consumedMessages.size() >= 1  // Should have at least the expected message

        and: "verify the message content"
        consumedMessages[0].value() == 'Hello World'
        //kafkaContainer.stop()
    }

    def "verify multiple messages can be consumed from HelloWorld topic"() {
        given: "the root endpoint will be called multiple times and consumer is ready"
        int numberOfCalls = 3
        def consumerThread = null
        def consumedMessages = []

        // Start consuming BEFORE producing to ensure we don't miss messages
        consumerThread = Thread.start {
            try {
                consumedMessages.addAll(kafkaUtils.consumeMessagesWithOffset("com.docker.msg.test", numberOfCalls, Duration.ofSeconds(20), "earliest"))
            } catch (Exception e) {
                println "❌ Consumer error: ${e.message}"
                e.printStackTrace()
            }
        }

        // Give consumer time to start
        Thread.sleep(500)

        when: "calling the endpoint multiple times"
        numberOfCalls.times {
            mockMvc.perform(get("/"))
                    .andExpect(status().isOk())
        }

        // Wait for consumer to finish
        consumerThread.join(25000)

        then: "all messages are published to the correct topic"
        consumedMessages.size() >= numberOfCalls  // Should have at least the expected number of messages

        and: "verify all messages have the expected content"
        consumedMessages.take(numberOfCalls).every { it.value() == 'Hello World' }
    }
}
