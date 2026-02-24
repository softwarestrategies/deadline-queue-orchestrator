# Tech Notes

## Prometheus

### Making sure that system's Prometheus is scraping the metrics

http://localhost:8080/actuator/prometheus

### Show Monitored Systems (Targets)
http://localhost:9090/targets

## Grafana

http://localhost:3000

### Create Dashboard from existing Grafana template

Create a new dashboard and import the template: 11378

### Create Custom Dashboard with custom-metrics panels

#### Events-related metrics

scheduledevent_events_executed_total

Update your Dockerfile for production``` dockerfile
# ... existing code ...

# Set production profile by default in container
ENV SPRING_PROFILES_ACTIVE=prod

ENTRYPOINT ["java", "-jar", "app.jar"]
```

5. Usage
Local development (default):``` bash
# Automatically uses dev profile
./mvnw spring-boot:run

# Or explicitly
./mvnw spring-boot:run -Dspring-boot.run.profiles=dev
```

Production:``` bash
# Via environment variable
export SPRING_PROFILES_ACTIVE=prod
java -jar target/scheduled-event-orchestrator.jar

# Or via command line
java -jar target/scheduled-event-orchestrator.jar --spring.profiles.active=prod
```

Docker Compose override (if needed):``` bash
docker-compose up --build  # Uses prod profile from Dockerfile
```
