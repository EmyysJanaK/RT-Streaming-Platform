package com.example.rtstreaming.service;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class StreamingLagMetrics {
    private final AtomicLong lag = new AtomicLong(0);
    private final ConsumerFactory<String, Object> consumerFactory;
    private final String topic;
    private final MeterRegistry meterRegistry;

    public StreamingLagMetrics(ConsumerFactory<String, Object> consumerFactory,
                               @Value("${kafka.topic:analytics-events}") String topic,
                               MeterRegistry meterRegistry) {
        this.consumerFactory = consumerFactory;
        this.topic = topic;
        this.meterRegistry = meterRegistry;
    }

    @PostConstruct
    public void init() {
        Gauge.builder("kafka_consumer_lag", lag, AtomicLong::get)
                .description("Kafka consumer lag for analytics-events topic")
                .tag("topic", topic)
                .register(meterRegistry);
    }

    // Poll lag every 10 seconds
    @Scheduled(fixedRate = 10000)
    public void updateLag() {
        try (Consumer<String, Object> consumer = consumerFactory.createConsumer()) {
            consumer.subscribe(Collections.singletonList(topic));
            consumer.poll(java.time.Duration.ofMillis(100));
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(consumer.assignment());
            Map<TopicPartition, Long> currentOffsets = consumer.position(consumer.assignment());
            long totalLag = 0;
            for (TopicPartition tp : consumer.assignment()) {
                long lagForPartition = endOffsets.getOrDefault(tp, 0L) - currentOffsets.getOrDefault(tp, 0L);
                totalLag += Math.max(lagForPartition, 0);
            }
            lag.set(totalLag);
        } catch (Exception ignored) {}
    }
}
