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

    public static void main(String[] args) throws StreamingQueryException {
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

        // Write the aggregated counts to the console (or log)
        // outputMode("update") ensures only updated counts are written per window
        // Checkpointing ensures state and progress are saved for recovery
        StreamingQuery query = windowedCounts
                .writeStream()
                .outputMode("update")
                .option("checkpointLocation", checkpointDir)
                .foreach(new ForeachWriter<Row>() {
                    // Open is called for each partition
                    @Override
                    public boolean open(long partitionId, long version) { return true; }
                    // Process each aggregated row (window, eventType, count)
                    @Override
                    public void process(Row value) {
                        logger.info("Window: {} | EventType: {} | Count: {}", value.getAs("window"), value.getAs("eventType"), value.getAs("count"));
                    }
                    // Close is called after all rows in the partition are processed
                    @Override
                    public void close(Throwable errorOrNull) { }
                })
                .start();

        // Await termination to keep the streaming job running
        query.awaitTermination();
    }
}
