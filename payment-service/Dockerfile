FROM eclipse-temurin:17-jdk-jammy
WORKDIR /app
COPY target/payment-service-*.jar app.jar
EXPOSE 8085
ENTRYPOINT ["java", "-jar", "app.jar"]