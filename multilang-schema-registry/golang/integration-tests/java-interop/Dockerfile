# Phase 4.6 Java <-> Go interop sidecar Docker image.
#
# Multi-stage build:
#   1. maven:3.9-eclipse-temurin-17  — Maven 3.9 + JDK 17 to compile + shade.
#   2. eclipse-temurin:21-jre        — slim JRE 21 to run the JAR.
#
# Tag locally as gsr-go-it-java-sidecar:latest; the Go testcontainers-go
# launcher picks the image up by that tag in container mode.

# ---- build stage ----
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
# Copy only the pom first so layer caching survives unrelated source edits.
COPY pom.xml ./
RUN mvn -B -DskipTests -q dependency:go-offline || true
COPY src ./src
RUN mvn -B -DskipTests -q package

# ---- run stage ----
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /src/target/java-interop-sidecar.jar /app/java-interop-sidecar.jar
# Container mode listens on a fixed port that the Go launcher maps via
# testcontainers-go's GetMappedPort. We pick 8080; the host port is dynamic.
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/java-interop-sidecar.jar", "--port=8080"]
