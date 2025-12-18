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

        Dataset<Row> events = df.selectExpr("CAST(value AS STRING) as json")
                .select(functions.from_json(functions.col("json"), schema).as("data"))
                .select("data.*");

        StreamingQuery query = events
                .writeStream()
                .outputMode("append")
                .option("checkpointLocation", checkpointDir)
                .foreach(new ForeachWriter<Row>() {
                    @Override
                    public boolean open(long partitionId, long version) { return true; }
                    @Override
                    public void process(Row value) {
                        logger.info("Received event: {}", value);
                    }
                    @Override
                    public void close(Throwable errorOrNull) { }
                })
                .start();

        query.awaitTermination();
    }
}
