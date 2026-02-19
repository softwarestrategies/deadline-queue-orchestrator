package io.softwarestrategies.scheduledevent.integration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Base test class with Testcontainers setup for PostgreSQL and Kafka.
 * Uses WebTestClient for modern Spring Boot 4 testing.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
@ActiveProfiles("test")
public abstract class BaseIntegrationTest {

	@Autowired
	protected WebTestClient webTestClient;

	@Container
	static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
			DockerImageName.parse("postgres:18-alpine"))
			.withDatabaseName("scheduledevent_test")
			.withUsername("test")
			.withPassword("test")
			.withCommand(
					"postgres",
					"-c", "max_connections=100",
					"-c", "shared_buffers=128MB"
			);

	@Container
	static KafkaContainer kafka = new KafkaContainer(
			DockerImageName.parse("confluentinc/cp-kafka:7.6.0"))
			.withKraft();

	@DynamicPropertySource
	static void configureProperties(DynamicPropertyRegistry registry) {
		// PostgreSQL
		registry.add("spring.datasource.url", postgres::getJdbcUrl);
		registry.add("spring.datasource.username", postgres::getUsername);
		registry.add("spring.datasource.password", postgres::getPassword);

		// Kafka
		registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
		registry.add("app.kafka.partitions", () -> "3");
		registry.add("app.kafka.replication-factor", () -> "1");

		// Scheduler settings for faster tests
		registry.add("app.scheduler.poll-interval-ms", () -> "100");
		registry.add("app.scheduler.batch-size", () -> "10");

		// Cleanup settings
		registry.add("app.cleanup.enabled", () -> "false");
	}
}