package io.softwarestrategies.scheduledevent.integration;

import io.softwarestrategies.scheduledevent.domain.DeliveryType;
import io.softwarestrategies.scheduledevent.domain.EventStatus;
import io.softwarestrategies.scheduledevent.domain.ScheduledEvent;
import io.softwarestrategies.scheduledevent.dto.ScheduledEventRequest;
import io.softwarestrategies.scheduledevent.repository.ScheduledEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration tests covering failure and edge-case paths for the REST API.
 *
 * Complements ScheduledEventApiIntegrationTest which only exercises happy paths.
 */
class ScheduledEventApiFailureIntegrationTest extends BaseIntegrationTest {

	@Autowired
	private ScheduledEventRepository repository;

	private static final String BASE_URL = "/api/v1/events";

	@BeforeEach
	void setUp() {
		repository.deleteAll();
	}

	// =========================================================================
	// Validation failures
	// =========================================================================

	@Test
	@DisplayName("Should reject request with missing externalJobId")
	void submitEvent_missingExternalJobId_returnsBadRequest() {
		ScheduledEventRequest request = ScheduledEventRequest.builder()
				.source("test-source")
				.scheduledAt(Instant.now().plus(1, ChronoUnit.HOURS))
				.deliveryType(DeliveryType.HTTP)
				.destination("https://example.com/webhook")
				.payload(Map.of("key", "value"))
				.build();

		webTestClient.post()
				.uri(BASE_URL)
				.contentType(MediaType.APPLICATION_JSON)
				.bodyValue(request)
				.exchange()
				.expectStatus().isBadRequest();
	}

	@Test
	@DisplayName("Should reject request with past scheduledAt")
	void submitEvent_pastScheduledAt_returnsBadRequest() {
		ScheduledEventRequest request = ScheduledEventRequest.builder()
				.externalJobId(UUID.randomUUID().toString())
				.source("test-source")
				.scheduledAt(Instant.now().minus(1, ChronoUnit.MINUTES))
				.deliveryType(DeliveryType.HTTP)
				.destination("https://example.com/webhook")
				.payload(Map.of("key", "value"))
				.build();

		webTestClient.post()
				.uri(BASE_URL)
				.contentType(MediaType.APPLICATION_JSON)
				.bodyValue(request)
				.exchange()
				.expectStatus().isBadRequest();
	}

	@Test
	@DisplayName("Should reject HTTP destination that does not start with http(s)://")
	void submitEvent_invalidHttpDestination_returnsBadRequest() {
		ScheduledEventRequest request = ScheduledEventRequest.builder()
				.externalJobId(UUID.randomUUID().toString())
				.source("test-source")
				.scheduledAt(Instant.now().plus(1, ChronoUnit.HOURS))
				.deliveryType(DeliveryType.HTTP)
				.destination("ftp://bad-protocol.example.com")
				.payload(Map.of("key", "value"))
				.build();

		webTestClient.post()
				.uri(BASE_URL)
				.contentType(MediaType.APPLICATION_JSON)
				.bodyValue(request)
				.exchange()
				.expectStatus().isBadRequest();
	}

	@Test
	@DisplayName("Should reject maxRetries above the allowed maximum of 10")
	void submitEvent_maxRetriesAboveLimit_returnsBadRequest() {
		ScheduledEventRequest request = ScheduledEventRequest.builder()
				.externalJobId(UUID.randomUUID().toString())
				.source("test-source")
				.scheduledAt(Instant.now().plus(1, ChronoUnit.HOURS))
				.deliveryType(DeliveryType.HTTP)
				.destination("https://example.com/webhook")
				.payload(Map.of("key", "value"))
				.maxRetries(11)
				.build();

		webTestClient.post()
				.uri(BASE_URL)
				.contentType(MediaType.APPLICATION_JSON)
				.bodyValue(request)
				.exchange()
				.expectStatus().isBadRequest();
	}

	@Test
	@DisplayName("Should reject negative maxRetries")
	void submitEvent_negativeMaxRetries_returnsBadRequest() {
		ScheduledEventRequest request = ScheduledEventRequest.builder()
				.externalJobId(UUID.randomUUID().toString())
				.source("test-source")
				.scheduledAt(Instant.now().plus(1, ChronoUnit.HOURS))
				.deliveryType(DeliveryType.HTTP)
				.destination("https://example.com/webhook")
				.payload(Map.of("key", "value"))
				.maxRetries(-1)
				.build();

		webTestClient.post()
				.uri(BASE_URL)
				.contentType(MediaType.APPLICATION_JSON)
				.bodyValue(request)
				.exchange()
				.expectStatus().isBadRequest();
	}

	// =========================================================================
	// Not-found paths
	// =========================================================================

	@Test
	@DisplayName("Should return 404 when getting event by unknown internal ID")
	void getEventById_unknownId_returnsNotFound() {
		webTestClient.get()
				.uri(BASE_URL + "/" + UUID.randomUUID())
				.exchange()
				.expectStatus().isNotFound();
	}

	@Test
	@DisplayName("Should return 404 when getting event by unknown external job ID")
	void getEventByExternalJobId_unknownId_returnsNotFound() {
		webTestClient.get()
				.uri(BASE_URL + "/external/does-not-exist")
				.exchange()
				.expectStatus().isNotFound();
	}

	@Test
	@DisplayName("Should return 404 when cancelling event with unknown external job ID")
	void cancelByExternalJobId_unknownId_returnsNotFound() {
		webTestClient.delete()
				.uri(BASE_URL + "/external/does-not-exist")
				.exchange()
				.expectStatus().isNotFound();
	}

	@Test
	@DisplayName("Should return 404 when cancelling event with unknown internal ID")
	void cancelById_unknownId_returnsNotFound() {
		webTestClient.delete()
				.uri(BASE_URL + "/" + UUID.randomUUID())
				.exchange()
				.expectStatus().isNotFound();
	}

	// =========================================================================
	// Business rule violations
	// =========================================================================

	@Test
	@DisplayName("Should return 404 when cancelling an already-cancelled event")
	void cancelEvent_alreadyCancelled_returnsNotFound() {
		// Given — persist a cancelled event directly
		ScheduledEvent event = ScheduledEvent.builder()
				.externalJobId("already-cancelled-job")
				.source("test-source")
				.scheduledAt(Instant.now().plus(1, ChronoUnit.HOURS))
				.deliveryType(DeliveryType.HTTP)
				.destination("https://example.com/webhook")
				.payload("{\"key\":\"value\"}")
				.status(EventStatus.CANCELLED)
				.retryCount(0)
				.maxRetries(3)
				.build();
		repository.save(event);

		// When — try to cancel again
		webTestClient.delete()
				.uri(BASE_URL + "/external/already-cancelled-job")
				.exchange()
				.expectStatus().isNotFound();
	}

	@Test
	@DisplayName("Should return 400 when cancelling an event that is already COMPLETED")
	void cancelById_completedEvent_returnsBadRequest() {
		// Given — persist a completed event directly
		ScheduledEvent event = ScheduledEvent.builder()
				.externalJobId("completed-job")
				.source("test-source")
				.scheduledAt(Instant.now().minus(1, ChronoUnit.HOURS))
				.deliveryType(DeliveryType.HTTP)
				.destination("https://example.com/webhook")
				.payload("{\"key\":\"value\"}")
				.status(EventStatus.COMPLETED)
				.retryCount(0)
				.maxRetries(3)
				.build();
		ScheduledEvent saved = repository.save(event);

		// When — try to cancel by internal ID
		webTestClient.delete()
				.uri(BASE_URL + "/" + saved.getId())
				.exchange()
				.expectStatus().isBadRequest();
	}

	// =========================================================================
	// Idempotency / deduplication
	// =========================================================================

	@Test
	@DisplayName("Duplicate submissions with the same key produce only one DB row")
	void submitEvent_duplicate_deduplicatedToSingleRow() {
		ScheduledEventRequest request = ScheduledEventRequest.builder()
				.externalJobId("dedup-job-" + UUID.randomUUID())
				.source("test-source")
				.scheduledAt(Instant.now().plus(2, ChronoUnit.HOURS))
				.deliveryType(DeliveryType.HTTP)
				.destination("https://example.com/webhook")
				.payload(Map.of("key", "value"))
				.build();

		// Submit twice
		webTestClient.post().uri(BASE_URL).contentType(MediaType.APPLICATION_JSON)
				.bodyValue(request).exchange().expectStatus().isAccepted();
		webTestClient.post().uri(BASE_URL).contentType(MediaType.APPLICATION_JSON)
				.bodyValue(request).exchange().expectStatus().isAccepted();

		// Wait for at least one row to appear
		await().atMost(10, TimeUnit.SECONDS)
				.pollInterval(500, TimeUnit.MILLISECONDS)
				.until(() -> repository.findByExternalJobId(request.getExternalJobId()).isPresent());

		// Allow a short grace period for any second write to land
		await().during(2, TimeUnit.SECONDS)
				.pollInterval(500, TimeUnit.MILLISECONDS)
				.untilAsserted(() ->
						assertThat(repository.findAllByExternalJobId(request.getExternalJobId()))
								.hasSize(1));
	}

	// =========================================================================
	// Destination format validation
	// =========================================================================

	@Test
	@DisplayName("Should accept a valid KAFKA topic name as destination")
	void submitEvent_kafkaDestination_accepted() {
		ScheduledEventRequest request = ScheduledEventRequest.builder()
				.externalJobId(UUID.randomUUID().toString())
				.source("test-source")
				.scheduledAt(Instant.now().plus(1, ChronoUnit.HOURS))
				.deliveryType(DeliveryType.KAFKA)
				.destination("my-external-topic")
				.payload(Map.of("key", "value"))
				.build();

		webTestClient.post()
				.uri(BASE_URL)
				.contentType(MediaType.APPLICATION_JSON)
				.bodyValue(request)
				.exchange()
				.expectStatus().isAccepted();
	}

	@Test
	@DisplayName("Should reject a KAFKA destination that contains spaces")
	void submitEvent_kafkaDestinationWithSpaces_returnsBadRequest() {
		ScheduledEventRequest request = ScheduledEventRequest.builder()
				.externalJobId(UUID.randomUUID().toString())
				.source("test-source")
				.scheduledAt(Instant.now().plus(1, ChronoUnit.HOURS))
				.deliveryType(DeliveryType.KAFKA)
				.destination("bad topic name")
				.payload(Map.of("key", "value"))
				.build();

		webTestClient.post()
				.uri(BASE_URL)
				.contentType(MediaType.APPLICATION_JSON)
				.bodyValue(request)
				.exchange()
				.expectStatus().isBadRequest();
	}

	// =========================================================================
	// Admin endpoint authentication
	// =========================================================================

	@Test
	@DisplayName("Admin cleanup endpoint should return 401 when no credentials provided")
	void adminCleanup_noCredentials_returnsUnauthorized() {
		webTestClient.post()
				.uri(BASE_URL + "/admin/cleanup")
				.exchange()
				.expectStatus().isUnauthorized();
	}

	@Test
	@DisplayName("Admin cleanup endpoint should return 401 when wrong credentials provided")
	void adminCleanup_wrongCredentials_returnsUnauthorized() {
		webTestClient.post()
				.uri(BASE_URL + "/admin/cleanup")
				.headers(h -> h.setBasicAuth("admin", "wrong-password"))
				.exchange()
				.expectStatus().isUnauthorized();
	}

	@Test
	@DisplayName("Admin cleanup endpoint should succeed with valid credentials")
	void adminCleanup_validCredentials_returnsOk() {
		adminWebTestClient().post()
				.uri(BASE_URL + "/admin/cleanup")
				.exchange()
				.expectStatus().isOk();
	}
}
