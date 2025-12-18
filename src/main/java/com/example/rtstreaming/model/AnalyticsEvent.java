package com.example.rtstreaming.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Value;

import java.time.Instant;
import java.util.Map;

@Value
@Builder
public class AnalyticsEvent {
    @NotBlank
    String eventId;

    @NotNull
    EventType eventType;

    @NotBlank
    String source;

    @NotNull
    Instant timestamp;

    @NotNull
    Map<String, Object> payload;

    @JsonCreator
    public AnalyticsEvent(
            @JsonProperty("eventId") String eventId,
            @JsonProperty("eventType") EventType eventType,
            @JsonProperty("source") String source,
            @JsonProperty("timestamp") Instant timestamp,
            @JsonProperty("payload") Map<String, Object> payload) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.source = source;
        this.timestamp = timestamp;
        this.payload = payload;
    }
}
