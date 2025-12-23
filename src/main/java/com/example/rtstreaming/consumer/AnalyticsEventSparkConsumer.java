package com.example.rtstreaming.consumer;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnalyticsEventSparkConsumer {
    private static final Logger logger = LoggerFactory.getLogger(AnalyticsEventSparkConsumer.class);

    public static void main(String[] args) throws StreamingQueryException, java.util.concurrent.TimeoutException {
        String kafkaBootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String topic = System.getenv().getOrDefault("KAFKA_TOPIC", "analytics-events");
        String checkpointDir = System.getenv().getOrDefault("SPARK_CHECKPOINT_DIR", "./spark-checkpoint");

        SparkSession spark = SparkSession.builder()
                .appName("AnalyticsEventSparkConsumer")
                .getOrCreate();

        StructType schema = new StructType()
                .add("eventId", DataTypes.StringType)
                .add("eventType", DataTypes.StringType)
                .add("source", DataTypes.StringType)
                .add("timestamp", DataTypes.TimestampType)
                .add("payload", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));

        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaBootstrap)
                .option("subscribe", topic)
                .option("startingOffsets", "latest")
                .load();

        // Parse Kafka value as JSON and extract event fields
        Dataset<Row> events = df.selectExpr("CAST(value AS STRING) as json")
                .select(functions.from_json(functions.col("json"), schema).as("data"))
                .select("data.*");

        // Add watermark to handle late events and enable state cleanup for fault tolerance
        // Watermark of 2 minutes means late events within 2 minutes of event time are allowed
        Dataset<Row> windowedCounts = events
                .withWatermark("timestamp", "2 minutes")
                // Tumbling window of 1 minute, group by eventType
                .groupBy(
                        functions.window(functions.col("timestamp"), "1 minute"),
                        functions.col("eventType")
                )
                .count();

                // Database connection properties are read from environment variables for decoupling
                String pgUrl = System.getenv().getOrDefault("PG_URL", "jdbc:postgresql://localhost:5432/analytics");
                String pgUser = System.getenv().getOrDefault("PG_USER", "postgres");
                String pgPassword = System.getenv().getOrDefault("PG_PASSWORD", "password");

                // Write the aggregated counts to PostgreSQL using foreachBatch for idempotent, batched writes
                // This approach is robust for production and supports retries and state recovery
                StreamingQuery query = windowedCounts
                                .writeStream()
                                .outputMode("update")
                                .option("checkpointLocation", checkpointDir)
                                .foreachBatch((batchDF, batchId) -> {
                                        // Add columns for window_start and window_end for DB schema
                                        Dataset<Row> outDF = batchDF
                                                .withColumn("window_start", functions.col("window.start"))
                                                .withColumn("window_end", functions.col("window.end"))
                                                .drop("window");

                                        // Retry logic for transient DB errors (up to 3 attempts)
                                        int maxRetries = 3;
                                        int attempt = 0;
                                        boolean success = false;
                                        Exception lastException = null;
                                        while (attempt < maxRetries && !success) {
                                                try {
                                                        // Use PostgreSQL upsert (ON CONFLICT) for idempotency
                                                        outDF.write()
                                                                .format("jdbc")
                                                                .option("url", pgUrl)
                                                                .option("dbtable", "aggregated_metrics")
                                                                .option("user", pgUser)
                                                                .option("password", pgPassword)
                                                                .option("driver", "org.postgresql.Driver")
                                                                .mode(SaveMode.Overwrite)
                                                                .option("truncate", false)
                                                                .option("batchsize", 1000)
                                                                .option("isolationLevel", "READ_COMMITTED")
                                                                .option("createTableOptions", "ON CONFLICT (window_start, window_end, event_type) DO UPDATE SET count = EXCLUDED.count")
                                                                .save();
                                                        success = true;
                                                } catch (Exception e) {
                                                        lastException = e;
                                                        logger.error("Failed to write batch {} to PostgreSQL (attempt {}/{}): {}", batchId, attempt+1, maxRetries, e.getMessage());
                                                        try { Thread.sleep(2000); } catch (InterruptedException ignored) {}
                                                }
                                                attempt++;
                                        }
                                        if (!success && lastException != null) {
                                                logger.error("Batch {} failed after {} attempts: {}", batchId, maxRetries, lastException.getMessage());
                                                throw lastException;
                                        }
                                })
                                .start();

                // Await termination to keep the streaming job running
                query.awaitTermination();
    }
}
