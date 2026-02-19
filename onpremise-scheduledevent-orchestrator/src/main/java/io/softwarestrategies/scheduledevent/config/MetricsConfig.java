package io.softwarestrategies.scheduledevent.config;

import io.micrometer.core.aop.TimedAspect;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.softwarestrategies.scheduledevent.domain.EventStatus;
import io.softwarestrategies.scheduledevent.repository.ScheduledEventRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Micrometer metrics configuration for monitoring event processing.
 */
@Configuration
public class MetricsConfig {

	private final AtomicLong pendingEventsGauge = new AtomicLong(0);
	private final AtomicLong processingEventsGauge = new AtomicLong(0);
	private final AtomicLong kafkaIngestionBacklog = new AtomicLong(0);

	@Bean
	public TimedAspect timedAspect(MeterRegistry registry) {
		return new TimedAspect(registry);
	}

	@Bean
	public Counter eventsReceivedCounter(MeterRegistry registry) {
		return Counter.builder("scheduledevent.events.received")
				.description("Total number of events received via REST API")
				.register(registry);
	}

	@Bean
	public Counter eventsPersistedCounter(MeterRegistry registry) {
		return Counter.builder("scheduledevent.events.persisted")
				.description("Total number of events persisted to database")
				.register(registry);
	}

	@Bean
	public Counter eventsExecutedCounter(MeterRegistry registry) {
		return Counter.builder("scheduledevent.events.executed")
				.description("Total number of events successfully executed")
				.register(registry);
	}

	@Bean
	public Counter eventsFailedCounter(MeterRegistry registry) {
		return Counter.builder("scheduledevent.events.failed")
				.description("Total number of event execution failures")
				.register(registry);
	}

	@Bean
	public Counter httpDeliveryCounter(MeterRegistry registry) {
		return Counter.builder("scheduledevent.delivery.http")
				.description("Total HTTP deliveries attempted")
				.register(registry);
	}

	@Bean
	public Counter kafkaDeliveryCounter(MeterRegistry registry) {
		return Counter.builder("scheduledevent.delivery.kafka")
				.description("Total Kafka deliveries attempted")
				.register(registry);
	}

	@Bean
	public Timer eventIngestionTimer(MeterRegistry registry) {
		return Timer.builder("scheduledevent.ingestion.time")
				.description("Time taken to ingest events")
				.publishPercentiles(0.5, 0.75, 0.95, 0.99)
				.register(registry);
	}

	@Bean
	public Timer eventDeliveryTimer(MeterRegistry registry) {
		return Timer.builder("scheduledevent.delivery.time")
				.description("Time taken to deliver events")
				.publishPercentiles(0.5, 0.75, 0.95, 0.99)
				.register(registry);
	}

	@Bean
	public Timer databaseBatchTimer(MeterRegistry registry) {
		return Timer.builder("scheduledevent.database.batch.time")
				.description("Time taken for database batch operations")
				.publishPercentiles(0.5, 0.75, 0.95, 0.99)
				.register(registry);
	}

	@Bean
	public Gauge pendingEventsMetric(MeterRegistry registry, ScheduledEventRepository repository) {
		return Gauge.builder("scheduledevent.events.pending", () -> {
					try {
						return repository.countByStatus(EventStatus.PENDING);
					} catch (Exception e) {
						return pendingEventsGauge.get();
					}
				})
				.description("Number of pending events")
				.register(registry);
	}

	@Bean
	public Gauge processingEventsMetric(MeterRegistry registry, ScheduledEventRepository repository) {
		return Gauge.builder("scheduledevent.events.processing", () -> {
					try {
						return repository.countByStatus(EventStatus.PROCESSING);
					} catch (Exception e) {
						return processingEventsGauge.get();
					}
				})
				.description("Number of events currently being processed")
				.register(registry);
	}

	@Bean
	public Gauge kafkaBacklogMetric(MeterRegistry registry) {
		return Gauge.builder("scheduledevent.kafka.backlog", kafkaIngestionBacklog::get)
				.description("Estimated Kafka ingestion backlog")
				.register(registry);
	}

	public void updatePendingEventsGauge(long count) {
		pendingEventsGauge.set(count);
	}

	public void updateProcessingEventsGauge(long count) {
		processingEventsGauge.set(count);
	}
}