package com.example.rtstreaming.producer;

import com.example.rtstreaming.model.AnalyticsEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

@Service
public class AnalyticsEventProducer {
    private static final Logger logger = LoggerFactory.getLogger(AnalyticsEventProducer.class);


    private final KafkaTemplate<String, AnalyticsEvent> kafkaTemplate;
    private final String topic;
    private final Timer latencyTimer;
    private final Counter throughputCounter;

    public AnalyticsEventProducer(KafkaTemplate<String, AnalyticsEvent> kafkaTemplate,
                                  @Value("${kafka.topic:analytics-events}") String topic,
                                  MeterRegistry meterRegistry,
                                  Timer kafkaProducerLatencyTimer) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
        this.latencyTimer = kafkaProducerLatencyTimer;
        this.throughputCounter = Counter.builder("kafka_producer_events_total")
                .description("Total number of events produced to Kafka")
                .tag("topic", topic)
                .register(meterRegistry);
    }

    public void sendEvent(AnalyticsEvent event) {
        long start = System.nanoTime();
        ListenableFuture<SendResult<String, AnalyticsEvent>> future =
                kafkaTemplate.send(topic, event.getEventId(), event);
        future.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onSuccess(SendResult<String, AnalyticsEvent> result) {
                long duration = System.nanoTime() - start;
                latencyTimer.record(duration, java.util.concurrent.TimeUnit.NANOSECONDS);
                throughputCounter.increment();
                logger.info("Sent event [{}] to topic {} partition {} offset {}", event.getEventId(),
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }

            @Override
            public void onFailure(Throwable ex) {
                long duration = System.nanoTime() - start;
                latencyTimer.record(duration, java.util.concurrent.TimeUnit.NANOSECONDS);
                logger.error("Failed to send event [{}]", event.getEventId(), ex);
            }
        });
    }
}
