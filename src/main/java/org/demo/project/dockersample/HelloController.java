package org.demo.project.dockersample;

import org.demo.project.dockersample.service.KafkaProducerService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HelloController {

    private final KafkaProducerService kafkaProducerService;

    public HelloController(KafkaProducerService kafkaProducerService) {
        this.kafkaProducerService = kafkaProducerService;
    }

    @GetMapping({"/hello", "/"})
    public String hello() {
        // Publish "Hello World" message to Kafka topic
        kafkaProducerService.sendMessage("Hello World");
        return "Hello World";
    }

    @PostMapping("/publish")
    public String publishMessage(@RequestParam(value = "message", defaultValue = "Hello World") String message) {
        kafkaProducerService.sendMessage(message);
        return "Message published to Kafka: " + message;
    }

    @PostMapping("/publish-with-key")
    public String publishMessageWithKey(
            @RequestParam(value = "key") String key,
            @RequestParam(value = "message", defaultValue = "Hello World") String message) {
        kafkaProducerService.sendMessageWithKey(key, message);
        return "Message published to Kafka with key '" + key + "': " + message;
    }
}

