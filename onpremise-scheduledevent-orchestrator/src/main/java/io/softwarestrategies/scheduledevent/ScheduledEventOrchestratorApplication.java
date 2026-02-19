package io.softwarestrategies.scheduledevent;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * On-Premise Scheduled Event Orchestrator
 *
 * High-throughput system designed to handle 10 million+ scheduled events per day.
 * Uses a two-phase ingestion pattern: REST API → Kafka → Database
 *
 * Key features:
 * - Virtual threads for optimal concurrency (Java 25 LTS)
 * - Kafka-based message buffering to handle thundering herd scenarios
 * - PostgreSQL 18 with partitioning for efficient querying
 * - SELECT FOR UPDATE SKIP LOCKED for distributed processing
 */
@SpringBootApplication
@EnableScheduling
public class ScheduledEventOrchestratorApplication {

	public static void main(String[] args) {
		SpringApplication.run(ScheduledEventOrchestratorApplication.class, args);
	}
}
