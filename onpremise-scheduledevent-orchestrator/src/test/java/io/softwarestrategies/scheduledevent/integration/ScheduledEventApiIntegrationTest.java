package io.softwarestrategies.scheduledevent.integration;

import io.softwarestrategies.scheduledevent.domain.DeliveryType;
import io.softwarestrategies.scheduledevent.domain.EventStatus;
import io.softwarestrategies.scheduledevent.dto.*;
import io.softwarestrategies.scheduledevent.repository.ScheduledEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration tests for the REST API endpoints.
 * Uses WebTestClient for modern Spring Boot 4 testing.
 */
class ScheduledEventApiIntegrationTest extends BaseIntegrationTest {

	@Autowired
	private ScheduledEventRepository repository;

	private static final String BASE_URL = "/api/v1/events";

	@BeforeEach
	void setUp() {
		repository.deleteAll();
	}

	@Test
	@DisplayName("Should submit a single event successfully")
	void submitSingleEvent() {
		// Given
		ScheduledEventRequest request = createTestRequest();

		// When/Then
		webTestClient.post()
				.uri(BASE_URL)
				.contentType(MediaType.APPLICATION_JSON)
				.bodyValue(request)
				.exchange()
				.expectStatus().isAccepted()
				.expectBody(ApiResponse.class)
				.value(response -> assertThat(response.isSuccess()).isTrue());

		// Wait for event to be persisted via Kafka
		await().atMost(10, TimeUnit.SECONDS)
				.pollInterval(500, TimeUnit.MILLISECONDS)
				.untilAsserted(() -> {
					assertThat(repository.findByExternalJobId(request.getExternalJobId()))
							.isPresent();
				});
	}

	@Test
	@DisplayName("Should submit a batch of events successfully")
	void submitBatchEvents() {
		// Given
		BatchScheduledEventRequest batchRequest = BatchScheduledEventRequest.builder()
				.events(List.of(
						createTestRequest(),
						createTestRequest(),
						createTestRequest()
				))
				.build();

		// When/Then
		webTestClient.post()
				.uri(BASE_URL + "/batch")
				.contentType(MediaType.APPLICATION_JSON)
				.bodyValue(batchRequest)
				.exchange()
				.expectStatus().isAccepted()
				.expectBody(ApiResponse.class)
				.value(response -> assertThat(response.isSuccess()).isTrue());

		// Wait for events to be persisted
		await().atMost(10, TimeUnit.SECONDS)
				.pollInterval(500, TimeUnit.MILLISECONDS)
				.untilAsserted(() -> {
					assertThat(repository.count()).isGreaterThanOrEqualTo(3);
				});
	}

	@Test
	@DisplayName("Should reject event with past scheduled time")
	void rejectPastScheduledTime() {
		// Given
		ScheduledEventRequest request = ScheduledEventRequest.builder()
				.externalJobId(UUID.randomUUID().toString())
				.source("test-source")
				.scheduledAt(Instant.now().minus(1, ChronoUnit.HOURS))
				.deliveryType(DeliveryType.HTTP)
				.destination("https://example.com/webhook")
				.payload(Map.of("key", "value"))
				.build();

		// When/Then
		webTestClient.post()
				.uri(BASE_URL)
				.contentType(MediaType.APPLICATION_JSON)
				.bodyValue(request)
				.exchange()
				.expectStatus().isBadRequest();
	}

	@Test
	@DisplayName("Should get event by ID after persistence")
	void getEventById() {
		// Given
		ScheduledEventRequest request = createTestRequest();

		webTestClient.post()
				.uri(BASE_URL)
				.contentType(MediaType.APPLICATION_JSON)
				.bodyValue(request)
				.exchange()
				.expectStatus().isAccepted();

		// Wait for persistence
		await().atMost(10, TimeUnit.SECONDS)
				.pollInterval(500, TimeUnit.MILLISECONDS)
				.until(() -> repository.findByExternalJobId(request.getExternalJobId()).isPresent());

		var event = repository.findByExternalJobId(request.getExternalJobId()).get();

		// When/Then
		webTestClient.get()
				.uri(BASE_URL + "/" + event.getId())
				.exchange()
				.expectStatus().isOk()
				.expectBody(ApiResponse.class)
				.value(response -> assertThat(response.isSuccess()).isTrue());
	}

	@Test
	@DisplayName("Should get event by external job ID")
	void getEventByExternalJobId() {
		// Given
		ScheduledEventRequest request = createTestRequest();

		webTestClient.post()
				.uri(BASE_URL)
				.contentType(MediaType.APPLICATION_JSON)
				.bodyValue(request)
				.exchange()
				.expectStatus().isAccepted();

		// Wait for persistence
		await().atMost(10, TimeUnit.SECONDS)
				.pollInterval(500, TimeUnit.MILLISECONDS)
				.until(() -> repository.findByExternalJobId(request.getExternalJobId()).isPresent());

		// When/Then
		webTestClient.get()
				.uri(BASE_URL + "/external/" + request.getExternalJobId())
				.exchange()
				.expectStatus().isOk()
				.expectBody(ApiResponse.class)
				.value(response -> assertThat(response.isSuccess()).isTrue());
	}

	@Test
	@DisplayName("Should cancel pending event")
	void cancelEvent() {
		// Given
		ScheduledEventRequest request = createTestRequest();

		webTestClient.post()
				.uri(BASE_URL)
				.contentType(MediaType.APPLICATION_JSON)
				.bodyValue(request)
				.exchange()
				.expectStatus().isAccepted();

		// Wait for persistence
		await().atMost(10, TimeUnit.SECONDS)
				.pollInterval(500, TimeUnit.MILLISECONDS)
				.until(() -> repository.findByExternalJobId(request.getExternalJobId()).isPresent());

		// When
		webTestClient.delete()
				.uri(BASE_URL + "/external/" + request.getExternalJobId())
				.exchange()
				.expectStatus().isOk();

		// Then
		await().atMost(5, TimeUnit.SECONDS)
				.pollInterval(500, TimeUnit.MILLISECONDS)
				.untilAsserted(() -> {
					var event = repository.findByExternalJobId(request.getExternalJobId()).get();
					assertThat(event.getStatus()).isEqualTo(EventStatus.CANCELLED);
				});
	}

	@Test
	@DisplayName("Should return 404 for non-existent event")
	void getNonExistentEvent() {
		webTestClient.get()
				.uri(BASE_URL + "/" + UUID.randomUUID())
				.exchange()
				.expectStatus().isNotFound();
	}

	@Test
	@DisplayName("Should get statistics")
	void getStatistics() {
		// Given - submit some events first
		for (int i = 0; i < 5; i++) {
			webTestClient.post()
					.uri(BASE_URL)
					.contentType(MediaType.APPLICATION_JSON)
					.bodyValue(createTestRequest())
					.exchange()
					.expectStatus().isAccepted();
		}

		// Wait for persistence
		await().atMost(10, TimeUnit.SECONDS)
				.pollInterval(500, TimeUnit.MILLISECONDS)
				.until(() -> repository.count() >= 5);

		// When/Then
		webTestClient.get()
				.uri(BASE_URL + "/statistics")
				.exchange()
				.expectStatus().isOk()
				.expectBody(ApiResponse.class)
				.value(response -> assertThat(response.isSuccess()).isTrue());
	}

	@Test
	@DisplayName("Should validate required fields")
	void validateRequiredFields() {
		// Given - request missing required fields
		ScheduledEventRequest request = ScheduledEventRequest.builder()
				.source("test-source")
				// Missing externalJobId, scheduledAt, deliveryType, destination, payload
				.build();

		// When/Then
		webTestClient.post()
				.uri(BASE_URL)
				.contentType(MediaType.APPLICATION_JSON)
				.bodyValue(request)
				.exchange()
				.expectStatus().isBadRequest();
	}

	@Test
	@DisplayName("Should handle health check")
	void healthCheck() {
		webTestClient.get()
				.uri(BASE_URL + "/health")
				.exchange()
				.expectStatus().isOk();
	}

	private ScheduledEventRequest createTestRequest() {
		return ScheduledEventRequest.builder()
				.externalJobId(UUID.randomUUID().toString())
				.source("test-source")
				.scheduledAt(Instant.now().plus(1, ChronoUnit.HOURS))
				.deliveryType(DeliveryType.HTTP)
				.destination("https://example.com/webhook")
				.payload(Map.of("message", "test", "timestamp", Instant.now().toString()))
				.maxRetries(3)
				.build();
	}
}