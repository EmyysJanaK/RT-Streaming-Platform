package com.example.rtstreaming.config;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MetricsConfig {
    @Bean
    public Timer kafkaProducerLatencyTimer(MeterRegistry registry) {
        return Timer.builder("kafka_producer_latency_seconds")
                .description("Kafka producer send latency in seconds")
                .publishPercentileHistogram()
                .register(registry);
    }
}
