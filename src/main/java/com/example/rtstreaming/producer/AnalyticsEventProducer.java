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

@Service
public class AnalyticsEventProducer {
    private static final Logger logger = LoggerFactory.getLogger(AnalyticsEventProducer.class);

    private final KafkaTemplate<String, AnalyticsEvent> kafkaTemplate;
    private final String topic;

    public AnalyticsEventProducer(KafkaTemplate<String, AnalyticsEvent> kafkaTemplate,
                                  @Value("${kafka.topic:analytics-events}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void sendEvent(AnalyticsEvent event) {
        ListenableFuture<SendResult<String, AnalyticsEvent>> future =
                kafkaTemplate.send(topic, event.getEventId(), event);
        future.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onSuccess(SendResult<String, AnalyticsEvent> result) {
                logger.info("Sent event [{}] to topic {} partition {} offset {}", event.getEventId(),
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }

            @Override
            public void onFailure(Throwable ex) {
                logger.error("Failed to send event [{}]", event.getEventId(), ex);
            }
        });
    }
}
