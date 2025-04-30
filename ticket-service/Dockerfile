FROM eclipse-temurin:17-jdk-jammy
WORKDIR /app
COPY target/ticket-service-*.jar app.jar
EXPOSE 8086
ENTRYPOINT ["java", "-jar", "app.jar"]