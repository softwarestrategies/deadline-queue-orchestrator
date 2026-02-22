package io.softwarestrategies.scheduledevent.performance;

import io.softwarestrategies.scheduledevent.domain.DeliveryType;
import io.softwarestrategies.scheduledevent.dto.ScheduledEventRequest;
import io.softwarestrategies.scheduledevent.integration.BaseIntegrationTest;
import io.softwarestrategies.scheduledevent.repository.ScheduledEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end ingestion throughput benchmark.
 *
 * Measures: REST API → Kafka → KafkaConsumerService → PostgreSQL
 *
 * Run with: mvn test -Pperformance -Dtest=IngestionThroughputTest
 *
 * Thresholds are intentionally conservative — this runs against real
 * Testcontainers infrastructure, not production hardware.
 */
@Tag("performance")
class IngestionThroughputTest extends BaseIntegrationTest {

	private static final Logger log = LoggerFactory.getLogger(IngestionThroughputTest.class);

	@Autowired
	private ScheduledEventRepository repository;

	/** Minimum acceptable end-to-end throughput through the full pipeline. */
	private static final int MIN_EVENTS_PER_SECOND = 50;

	/** Maximum acceptable p99 API response time for a single submit. */
	private static final long MAX_P99_SUBMIT_MS = 500;

	@BeforeEach
	void setUp() {
		repository.deleteAll();
	}

	// -------------------------------------------------------------------------
	// Single-event submission latency
	// -------------------------------------------------------------------------

	@Test
	void singleEventSubmit_p99LatencyUnder500ms() {
		int samples = 100;
		List<Long> latenciesMs = new ArrayList<>(samples);

		// Warm up — first request may be slower due to lazy init
		submitSingle();

		for (int i = 0; i < samples; i++) {
			long start = System.nanoTime();
			submitSingle();
			latenciesMs.add(Duration.ofNanos(System.nanoTime() - start).toMillis());
		}

		latenciesMs.sort(Long::compareTo);
		long p50 = latenciesMs.get(samples / 2);
		long p99 = latenciesMs.get((int) (samples * 0.99));

		log.info("[IngestionLatency] samples={} p50={}ms p99={}ms min={}ms max={}ms",
				samples, p50, p99, latenciesMs.get(0), latenciesMs.get(samples - 1));

		assertThat(p99)
				.as("p99 submit latency should be under %dms but was %dms", MAX_P99_SUBMIT_MS, p99)
				.isLessThanOrEqualTo(MAX_P99_SUBMIT_MS);
	}

	// -------------------------------------------------------------------------
	// Concurrent batch submission throughput
	// -------------------------------------------------------------------------

	@Test
	void concurrentIngestion_500events_meetsMinimumThroughput() {
		int totalEvents = 500;
		List<String> externalJobIds = new ArrayList<>(totalEvents);

		// Build all requests up front
		List<ScheduledEventRequest> requests = new ArrayList<>(totalEvents);
		for (int i = 0; i < totalEvents; i++) {
			String jobId = "perf-job-" + UUID.randomUUID();
			externalJobIds.add(jobId);
			requests.add(ScheduledEventRequest.builder()
					.externalJobId(jobId)
					.source("perf-test")
					.scheduledAt(Instant.now().plus(1, ChronoUnit.HOURS))
					.deliveryType(DeliveryType.HTTP)
					.destination("https://example.com/perf-webhook")
					.payload(Map.of("index", i, "ts", Instant.now().toString()))
					.maxRetries(0)
					.build());
		}

		// Submit all concurrently using virtual threads
		Instant submitStart = Instant.now();
		List<CompletableFuture<Void>> futures = requests.stream()
				.map(req -> CompletableFuture.runAsync(() -> submitSingleRequest(req)))
				.toList();
		CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
		long submitMs = Duration.between(submitStart, Instant.now()).toMillis();

		log.info("[IngestionThroughput] Submitted {} events in {}ms ({} req/s)",
				totalEvents, submitMs, totalEvents * 1000L / Math.max(submitMs, 1));

		// Wait for the full pipeline to drain (REST → Kafka → DB)
		Instant pipelineStart = Instant.now();
		await().atMost(60, TimeUnit.SECONDS)
				.pollInterval(500, TimeUnit.MILLISECONDS)
				.untilAsserted(() ->
						assertThat(repository.count())
								.as("All %d events should be persisted", totalEvents)
								.isGreaterThanOrEqualTo(totalEvents));

		long totalPipelineMs = Duration.between(pipelineStart, Instant.now()).toMillis() + submitMs;
		double throughput = totalEvents * 1000.0 / totalPipelineMs;

		log.info("[IngestionThroughput] Pipeline drained in {}ms total — {:.1f} events/sec",
				totalPipelineMs, throughput);

		assertThat(throughput)
				.as("End-to-end throughput should exceed %d events/sec but was %.1f",
						MIN_EVENTS_PER_SECOND, throughput)
				.isGreaterThanOrEqualTo(MIN_EVENTS_PER_SECOND);
	}

	// -------------------------------------------------------------------------
	// Deduplication overhead
	// -------------------------------------------------------------------------

	@Test
	void duplicateIngestion_dedupOverhead_acceptable() {
		// Submit 50 unique events
		int unique = 50;
		List<ScheduledEventRequest> uniqueRequests = new ArrayList<>(unique);
		for (int i = 0; i < unique; i++) {
			uniqueRequests.add(buildRequest("dedup-job-" + i));
		}

		uniqueRequests.forEach(this::submitSingleRequest);

		// Wait for all unique events to land
		await().atMost(30, TimeUnit.SECONDS)
				.pollInterval(500, TimeUnit.MILLISECONDS)
				.until(() -> repository.count() >= unique);

		// Now re-submit all the same events (full duplicates)
		Instant dedupStart = Instant.now();
		uniqueRequests.forEach(this::submitSingleRequest);
		long dedupSubmitMs = Duration.between(dedupStart, Instant.now()).toMillis();

		// Give the pipeline time to process the duplicates
		try { Thread.sleep(3000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

		long finalCount = repository.count();
		log.info("[DedupOverhead] Re-submitted {} duplicates in {}ms. DB count stayed at {} (expected {})",
				unique, dedupSubmitMs, finalCount, unique);

		// Dedup must hold — no extra rows
		assertThat(finalCount)
				.as("Duplicate submissions must not create extra rows")
				.isEqualTo(unique);
	}

	// -------------------------------------------------------------------------
	// Helpers
	// -------------------------------------------------------------------------

	private void submitSingle() {
		submitSingleRequest(buildRequest(UUID.randomUUID().toString()));
	}

	private void submitSingleRequest(ScheduledEventRequest request) {
		webTestClient.post()
				.uri("/api/v1/events")
				.contentType(MediaType.APPLICATION_JSON)
				.bodyValue(request)
				.exchange()
				.expectStatus().isAccepted();
	}

	private ScheduledEventRequest buildRequest(String jobId) {
		return ScheduledEventRequest.builder()
				.externalJobId(jobId)
				.source("perf-test")
				.scheduledAt(Instant.now().plus(1, ChronoUnit.HOURS))
				.deliveryType(DeliveryType.HTTP)
				.destination("https://example.com/perf-webhook")
				.payload(Map.of("key", "value"))
				.maxRetries(0)
				.build();
	}
}
