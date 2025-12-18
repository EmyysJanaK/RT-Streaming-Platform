package com.example.rtstreaming.service;

import org.springframework.stereotype.Component;

@Component
public class AnalyticsWebSocketBroadcaster {
    private final AnalyticsWebSocketHandler handler;

    public AnalyticsWebSocketBroadcaster() {
        this.handler = new AnalyticsWebSocketHandler();
    }

    public void broadcast(String message) {
        handler.broadcast(message);
    }

    public AnalyticsWebSocketHandler getHandler() {
        return handler;
    }
}
