FROM eclipse-temurin-23
WORKDIR /app
COPY target/redisx.jar .
EXPOSE 6379
CMD ["java", "-jar", "redisx.jar"]
