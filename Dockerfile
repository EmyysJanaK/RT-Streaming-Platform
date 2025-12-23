# Dockerfile for Spring Boot backend
FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY target/rt-streaming-platform-*.jar app.jar
EXPOSE 8080
ENV JAVA_OPTS=""
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
