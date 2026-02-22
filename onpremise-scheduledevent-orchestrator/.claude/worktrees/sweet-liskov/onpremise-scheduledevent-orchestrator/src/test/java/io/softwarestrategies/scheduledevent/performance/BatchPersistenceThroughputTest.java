package io.softwarestrategies.scheduledevent.performance;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.softwarestrategies.scheduledevent.domain.DeliveryType;
import io.softwarestrategies.scheduledevent.domain.EventStatus;
import io.softwarestrategies.scheduledevent.domain.ScheduledEvent;
import io.softwarestrategies.scheduledevent.dto.KafkaEventMessage;
import io.softwarestrategies.scheduledevent.integration.BaseIntegrationTest;
import io.softwarestrategies.scheduledevent.repository.ScheduledEventRepository;
import io.softwarestrategies.scheduledevent.service.KafkaConsumerService;
import io.softwarestrategies.scheduledevent.service.KafkaProducerService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.Acknowledgment;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Batch persistence throughput benchmark.
 *
 * Directly exercises KafkaConsumerService against a real PostgreSQL instance
 * (via Testcontainers) to measure raw insert throughput at different batch sizes,
 * without the overhead of the Kafka pipeline.
 *
 * Run with: mvn test -Pperformance -Dtest=BatchPersistenceThroughputTest
 */
@Tag("performance")
class BatchPersistenceThroughputTest extends BaseIntegrationTest {

	private static final Logger log = LoggerFactory.getLogger(BatchPersistenceThroughputTest.class);

	@Autowired
	private ScheduledEventRepository repository;

	@Autowired
	private KafkaProducerService kafkaProducerService;

	/** Minimum rows/sec the DB layer must sustain at any tested batch size. */
	private static final int MIN_ROWS_PER_SECOND = 200;

	private KafkaConsumerService consumerService;
	private Acknowledgment noopAck;

	@BeforeEach
	void setUp() {
		repository.deleteAll();

		MeterRegistry registry = new SimpleMeterRegistry();
		Counter persisted = Counter.builder("test.persisted").register(registry);
		Counter cacheHit = Counter.builder("test.cache.hit").register(registry);
		Counter dbHit = Counter.builder("test.db.hit").register(registry);
		Timer batchTimer = Timer.builder("test.batch").register(registry);

		consumerService = new KafkaConsumerService(
				repository, kafkaProducerService,
				persisted, cacheHit, dbHit, batchTimer);

		noopAck = mock(Acknowledgment.class);
	}

	// -------------------------------------------------------------------------
	// Batch size = 100
	// -------------------------------------------------------------------------

	@Test
	void persistBatch_100events_meetsMinimumThroughput() {
		runBatchBenchmark(100, 10);
	}

	// -------------------------------------------------------------------------
	// Batch size = 500
	// -------------------------------------------------------------------------

	@Test
	void persistBatch_500events_meetsMinimumThroughput() {
		runBatchBenchmark(500, 5);
	}

	// -------------------------------------------------------------------------
	// Scheduler read path: SELECT FOR UPDATE SKIP LOCKED throughput
	// -------------------------------------------------------------------------

	@Test
	void fetchDueEvents_1000pendingRows_batchReadUnder100ms() {
		// Insert 1000 PENDING events with scheduled_at in the past
		List<ScheduledEvent> events = new ArrayList<>(1000);
		Instant past = Instant.now().minus(1, ChronoUnit.MINUTES);
		for (int i = 0; i < 1000; i++) {
			events.add(ScheduledEvent.builder()
					.externalJobId("read-perf-" + i)
					.source("perf-test")
					.scheduledAt(past)
					.deliveryType(DeliveryType.HTTP)
					.destination("https://example.com/webhook")
					.payload("{\"i\":" + i + "}")
					.status(EventStatus.PENDING)
					.retryCount(0)
					.maxRetries(3)
					.build());
		}
		repository.saveAll(events);
		assertThat(repository.count()).isEqualTo(1000);

		// Warm up
		repository.findAndLockEventsForProcessing(Instant.now(), Instant.now(), 1);

		// Measure a batch read of 100
		long[] latenciesNs = new long[20];
		for (int i = 0; i < latenciesNs.length; i++) {
			// Reset all rows back to PENDING between reads
			repository.releaseExpiredLocks(Instant.now().plus(10, ChronoUnit.MINUTES));

			long start = System.nanoTime();
			List<ScheduledEvent> batch = repository.findAndLockEventsForProcessing(
					Instant.now(), Instant.now(), 100);
			latenciesNs[i] = System.nanoTime() - start;

			assertThat(batch).hasSize(100);
		}

		long p50Ms = Duration.ofNanos(sortedPercentile(latenciesNs, 50)).toMillis();
		long p99Ms = Duration.ofNanos(sortedPercentile(latenciesNs, 99)).toMillis();

		log.info("[SelectForUpdate] p50={}ms p99={}ms (batch=100, pool=1000 rows)",
				p50Ms, p99Ms);

		assertThat(p99Ms)
				.as("SELECT FOR UPDATE SKIP LOCKED p99 should be under 100ms but was %dms", p99Ms)
				.isLessThanOrEqualTo(100);
	}

	// -------------------------------------------------------------------------
	// Helpers
	// -------------------------------------------------------------------------

	/**
	 * Insert {@code batchSize} events via KafkaConsumerService in {@code rounds} rounds,
	 * then assert the observed rows/sec meets the minimum threshold.
	 */
	private void runBatchBenchmark(int batchSize, int rounds) {
		long totalInserted = 0;
		long totalMs = 0;

		for (int round = 0; round < rounds; round++) {
			List<KafkaEventMessage> batch = buildBatch(batchSize);

			long start = System.nanoTime();
			consumerService.consumeIngestionBatch(batch, noopAck);
			long elapsedMs = Duration.ofNanos(System.nanoTime() - start).toMillis();

			totalInserted += batchSize;
			totalMs += elapsedMs;

			log.debug("[BatchPersistence] round={} batchSize={} elapsed={}ms rows/s={}",
					round + 1, batchSize, elapsedMs,
					batchSize * 1000L / Math.max(elapsedMs, 1));
		}

		double rowsPerSec = totalInserted * 1000.0 / Math.max(totalMs, 1);

		log.info("[BatchPersistence] batchSize={} rounds={} total={}rows totalMs={}ms → {:.0f} rows/sec",
				batchSize, rounds, totalInserted, totalMs, rowsPerSec);

		assertThat(rowsPerSec)
				.as("Batch persistence should exceed %d rows/sec at batchSize=%d but was %.0f",
						MIN_ROWS_PER_SECOND, batchSize, rowsPerSec)
				.isGreaterThanOrEqualTo(MIN_ROWS_PER_SECOND);

		assertThat(repository.count())
				.as("All %d inserted events must be in the DB", totalInserted)
				.isEqualTo(totalInserted);
	}

	private List<KafkaEventMessage> buildBatch(int size) {
		List<KafkaEventMessage> batch = new ArrayList<>(size);
		// Use a future time to avoid @Future validation (not relevant here — we call service directly)
		Instant scheduledAt = Instant.now().plus(1, ChronoUnit.HOURS);
		for (int i = 0; i < size; i++) {
			KafkaEventMessage msg = new KafkaEventMessage();
			msg.setMessageId(UUID.randomUUID().toString());
			msg.setExternalJobId(UUID.randomUUID().toString());
			msg.setSource("perf-test");
			msg.setScheduledAt(scheduledAt.plusSeconds(i)); // unique per row
			msg.setDeliveryType(DeliveryType.HTTP);
			msg.setDestination("https://example.com/webhook");
			msg.setPayload("{\"key\":\"value\"}");
			msg.setMaxRetries(3);
			batch.add(msg);
		}
		return batch;
	}

	private long sortedPercentile(long[] values, int percentile) {
		long[] sorted = values.clone();
		java.util.Arrays.sort(sorted);
		int index = (int) Math.ceil(percentile / 100.0 * sorted.length) - 1;
		return sorted[Math.max(0, index)];
	}
}
