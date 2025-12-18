package com.example.rtstreaming.service;

import org.springframework.stereotype.Service;

@Service
public class HealthService {
    public String getHealthStatus() {
        return "UP";
    }
}
