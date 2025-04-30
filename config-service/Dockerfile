FROM eclipse-temurin:17-jdk-jammy
WORKDIR /app
COPY target/config-service-*.jar app.jar
EXPOSE 8888
ENTRYPOINT ["java", "-jar", "app.jar"]