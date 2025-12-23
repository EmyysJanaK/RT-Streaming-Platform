# Dockerfile for Spark Structured Streaming job
FROM bitnami/spark:3.5.0
WORKDIR /app
COPY target/rt-streaming-platform-*.jar app.jar
ENV SPARK_APPLICATION_JAR_LOCATION=/app/app.jar
ENV SPARK_APPLICATION_MAIN_CLASS=com.example.rtstreaming.consumer.AnalyticsEventSparkConsumer
CMD ["/opt/bitnami/scripts/spark/entrypoint.sh", "/opt/bitnami/scripts/spark/run.sh"]
