-- SQL schema for storing aggregated metrics from Spark streaming
CREATE TABLE IF NOT EXISTS aggregated_metrics (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    event_type VARCHAR(32) NOT NULL,
    count BIGINT NOT NULL,
    PRIMARY KEY (window_start, window_end, event_type)
);

-- Optimized index for analytics queries by event_type and time range
CREATE INDEX IF NOT EXISTS idx_aggregated_metrics_event_type_time
    ON aggregated_metrics (event_type, window_start, window_end);
