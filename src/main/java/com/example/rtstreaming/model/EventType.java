package com.example.rtstreaming.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum EventType {
    LOG,
    METRIC,
    CLICK;

    @JsonCreator
    public static EventType fromValue(String value) {
        return EventType.valueOf(value.toUpperCase());
    }

    @JsonValue
    public String toValue() {
        return this.name();
    }
}
