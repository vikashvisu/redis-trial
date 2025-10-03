# Multi-stage build: First stage compiles, second runs
FROM maven:4.0.0-eclipse-temurin-23 AS build
WORKDIR /app
COPY . .
RUN mvn clean package -DskipTests

# Runtime stage
FROM eclipse-temurin:23-jdk
WORKDIR /app
COPY --from=build /app/target/*.jar redisx.jar
EXPOSE 6379
CMD ["java", "-jar", "redisx.jar"]
