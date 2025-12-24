package com.example.rtstreaming.controller;

import com.example.rtstreaming.model.AnalyticsEvent;
import com.example.rtstreaming.model.EventType;
import com.example.rtstreaming.producer.AnalyticsEventProducer;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/producer")
public class ProducerController {
    private static final Logger logger = LoggerFactory.getLogger(ProducerController.class);
    private final AnalyticsEventProducer producer;

    public ProducerController(AnalyticsEventProducer producer) {
        this.producer = producer;
    }

    @PostMapping("/event")
    public ResponseEntity<String> publishEvent(@Valid @RequestBody AnalyticsEvent event) {
        producer.sendEvent(event);
        return ResponseEntity.accepted().body("Event published");
    }

    @PostMapping("/event/sample")
    public ResponseEntity<String> publishSampleEvent() {
        Map<String, Object> payload = new HashMap<>();
        payload.put("message", "Sample event");
        AnalyticsEvent event = new AnalyticsEvent(
            UUID.randomUUID().toString(),
            EventType.LOG,
            "test-api",
            Instant.now(),
            payload
        );
        logger.info("Publishing sample event: {}", event);
        producer.sendEvent(event);
        return ResponseEntity.accepted().body("Sample event published");
    }
}
