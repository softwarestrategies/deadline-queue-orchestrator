# Scheduled Event Orchestrator

A production-grade, high-throughput Scheduled Event Orchestration service designed for 10M+ events/day and flexible enough to work with several client systems willing to abide to its API guidelines

I needed this for another personal project that I am working on.  Plus it seemed like a fun challenge.  And I am hoping (unlikely as it maybe be) that I will be able to use it as a basis (or inspiration) for a production-grade microservice for the company that I am currently working for.

**This was built by me, helped (significantly) by using Claude Code for reviewing, debugging and testing.**

## Tech Stack

- **Java 25** (LTS) with Virtual Threads
- **Spring Boot 4.0.2** (Spring Framework 7)
- **Apache Kafka** (KRaft mode) for message buffering
- **PostgreSQL 18** with range partitioning
- **Micrometer + Prometheus + Grafana** for monitoring & observability
- **Docker Compose** for deployment
- **Testcontainers** for integration testing

## Architecture

### Core Components

There are 5 primary services with clear single responsibilities:

- **ScheduledEventService** - Orchestration + validation
- **EventSchedulerService** - Polling loop (@Scheduled + SELECT FOR UPDATE SKIP LOCKED)
- **EventPersistenceService** - DB state management
- **EventDeliveryService** - HTTP/Kafka delivery with retry logic
- **KafkaConsumerService** - Batch ingestion + deduplication

### Two-Phase Ingestion Pattern

```
REST API → Kafka (ingestion) → PostgreSQL (persistence) → Scheduler (delivery via HTTP or Kafka)
```

This pattern provides:
- **Backpressure handling** for traffic spikes
- **Ordering guarantees** per client via partition keys
- **Asynchronous processing** with virtual threads
- **Distributed processing** with SELECT FOR UPDATE SKIP LOCKED

### Design Strengths

- Distributed locking via SELECT FOR UPDATE SKIP LOCKED — solid approach for multi-pod deployments
- Two-layer deduplication: in-memory LRU (100K entries) + DB unique constraint
- Range partitioning on partition_key (date-based) for efficient cleanup and query pruning
- Virtual threads throughout for I/O-bound concurrency — appropriate for this workload
- Dead Letter Queue strategy is well-implemented
- Clean separation: controller → service → repository

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Java 25 JDK (for development)
- Maven 3.9+

### Run with Docker Compose

```bash
# Start all services
docker-compose up --build

# Or start in detached mode
docker-compose up -d --build
```

### Access Points

| Service | URL |
|---------|-----|
| Application | http://localhost:8080 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 (admin/admin) |
| Actuator | http://localhost:8080/actuator |
| Metrics | http://localhost:8080/actuator/prometheus |

## API Endpoints

### Submit Single Event
```bash
POST /api/v1/events
Content-Type: application/json

{
  "external_job_id": "order-12345",
  "source": "order-service",
  "scheduled_at": "2026-02-24T15:18:00-08:00",
  "delivery_type": "HTTP",
  "destination": "http://localhost:8080/api/v1/test-events",
  "payload": {
    "orderId": "12345",
    "customerId": "cust-789hi.",
    "action": "process_payment",
    "amount": 99.99,
    "currency": "USD"
  },
  "max_retries": 3
}
```

### Submit Batch Events
```bash
POST /api/v1/events/batch
Content-Type: application/json

{
  "events": [
    { ... },
    { ... }
  ]
}
```

### Query Events
```bash
GET /api/v1/events/{id}
GET /api/v1/events/external/{externalJobId}
GET /api/v1/events/external/{externalJobId}/all
GET /api/v1/events/statistics
```

### Cancel Event
```bash
DELETE /api/v1/events/{id}
DELETE /api/v1/events/external/{externalJobId}
```

## Configuration

Key environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| DB_HOST | localhost | PostgreSQL host |
| DB_PORT | 5432 | PostgreSQL port |
| KAFKA_BOOTSTRAP_SERVERS | localhost:9092 | Kafka brokers |
| KAFKA_PARTITIONS | 24 | Number of partitions |
| SCHEDULER_POLL_INTERVAL_MS | 1000 | Scheduler poll interval |
| SCHEDULER_BATCH_SIZE | 100 | Events per batch |
| CLEANUP_RETENTION_DAYS | 7 | Event retention period |

## Performance Optimizations

1. **Kafka Batching**: 32KB batch size, 10ms linger, lz4 compression
2. **Virtual Threads**: Optimal for I/O-bound operations (Java 25)
3. **PostgreSQL 18 Partitioning**: RANGE by date for efficient queries
4. **Connection Pooling**: HikariCP (50 connections), WebClient (200 connections)
5. **Batch Inserts**: JPA batch size of 100
6. **Distributed Locking**: SELECT FOR UPDATE SKIP LOCKED

## Scaling for 10M Events/Day

Expected load: ~115 events/second (average), peaks of 1,000+ events/second

Recommended settings:
- Kafka partitions: 24-48
- Consumer concurrency: 10-20
- Scheduler batch size: 100-500
- DB connection pool: 50-100

## Running Tests

```bash
# Run integration tests with Testcontainers
./mvnw test

# Skip tests
./mvnw package -DskipTests
```

## Project Structure

```
src/main/java/io/softwarestrategies/scheduledevent/
├── config/          # Spring configurations
├── controller/      # REST endpoints
├── domain/          # JPA entities
├── dto/             # Request/response objects
├── exception/       # Custom exceptions
├── kafka/           # Kafka producer/consumer
├── repository/      # JPA repositories
├── service/         # Business logic
└── ScheduledEventOrchestratorApplication.java
```

## Monitoring

The Grafana dashboard provides:
- Event throughput (received, persisted, executed, failed)
- Delivery metrics by type (HTTP/Kafka)
- Latency percentiles (p50, p95, p99)
- JVM metrics (heap, threads, GC)
- Kafka consumer lag

## License

MIT License

📌 
🔎
🛠
⭐
