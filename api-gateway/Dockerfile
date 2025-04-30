FROM eclipse-temurin:17-jdk-jammy
WORKDIR /app
COPY target/api-gateway-*.jar app.jar
EXPOSE 8083
ENTRYPOINT ["java", "-jar", "app.jar"]