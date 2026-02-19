package io.softwarestrategies.scheduledevent.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.softwarestrategies.scheduledevent.domain.ScheduledEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.Set;

/**
 * Service responsible for delivering scheduled events to their destinations.
 * Supports both HTTP POST and Kafka topic delivery.
 * Uses RestClient with virtual threads for efficient blocking I/O.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class EventDeliveryService {

	private final RestClient restClient;
	private final KafkaProducerService kafkaProducerService;
	private final Counter httpDeliveryCounter;
	private final Counter kafkaDeliveryCounter;
	private final Timer eventDeliveryTimer;

	private static final Set<Integer> RETRIABLE_STATUS_CODES = Set.of(408, 429, 500, 502, 503, 504);
	private static final int MAX_RETRIES = 2;

	/**
	 * Deliver an event to its destination.
	 */
	public DeliveryResult deliverEvent(ScheduledEvent event) {
		Timer.Sample sample = Timer.start();

		try {
			return switch (event.getDeliveryType()) {
				case HTTP -> deliverViaHttp(event);
				case KAFKA -> deliverViaKafka(event);
			};
		} finally {
			sample.stop(eventDeliveryTimer);
		}
	}

	/**
	 * Deliver event via HTTP POST with retry logic.
	 */
	private DeliveryResult deliverViaHttp(ScheduledEvent event) {
		httpDeliveryCounter.increment();

		Exception lastException = null;

		for (int attempt = 0; attempt <= MAX_RETRIES; attempt++) {
			try {
				ResponseEntity<Void> response = restClient.post()
						.uri(event.getDestination())
						.contentType(MediaType.APPLICATION_JSON)
						.body(event.getPayload())
						.retrieve()
						.toBodilessEntity();

				log.debug("HTTP delivery successful. EventId: {}, Status: {}",
						event.getId(), response.getStatusCode());
				return DeliveryResult.ofSuccess();

			} catch (RestClientResponseException ex) {
				lastException = ex;
				if (!isRetriableStatusCode(ex.getStatusCode().value())) {
					break;  // Non-retriable error, don't retry
				}
				if (attempt < MAX_RETRIES) {
					sleep(500 * (attempt + 1));  // Backoff
				}
			} catch (Exception ex) {
				lastException = ex;
				if (!isRetriableException(ex)) {
					break;
				}
				if (attempt < MAX_RETRIES) {
					sleep(500 * (attempt + 1));
				}
			}
		}

		String error = extractErrorMessage(lastException);
		boolean retriable = isRetriable(lastException);
		log.warn("HTTP delivery failed. EventId: {}, Retriable: {}, Error: {}",
				event.getId(), retriable, error);
		return DeliveryResult.ofFailure(error, retriable);
	}

	/**
	 * Deliver event via Kafka topic.
	 */
	private DeliveryResult deliverViaKafka(ScheduledEvent event) {
		kafkaDeliveryCounter.increment();

		try {
			kafkaProducerService.sendToExternalTopic(
					event.getDestination(),
					event.getExternalJobId(),
					event.getPayload()
			).join();  // Block on virtual thread

			log.debug("Kafka delivery successful. EventId: {}, Topic: {}",
					event.getId(), event.getDestination());
			return DeliveryResult.ofSuccess();

		} catch (Exception ex) {
			String error = "Kafka delivery failed: " + ex.getMessage();
			log.warn("Kafka delivery failed. EventId: {}, Error: {}", event.getId(), error);
			return DeliveryResult.ofFailure(error, true);
		}
	}

	private boolean isRetriableStatusCode(int statusCode) {
		return RETRIABLE_STATUS_CODES.contains(statusCode);
	}

	private boolean isRetriableException(Exception ex) {
		Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
		return cause instanceof ConnectException || cause instanceof SocketTimeoutException;
	}

	private boolean isRetriable(Exception ex) {
		if (ex instanceof RestClientResponseException rcEx) {
			return isRetriableStatusCode(rcEx.getStatusCode().value());
		}
		return isRetriableException(ex);
	}

	private String extractErrorMessage(Exception ex) {
		if (ex instanceof RestClientResponseException rcEx) {
			return String.format("HTTP %d: %s", rcEx.getStatusCode().value(), rcEx.getStatusText());
		}
		return ex.getMessage() != null ? ex.getMessage() : ex.getClass().getSimpleName();
	}

	private void sleep(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	/**
	 * Result of a delivery attempt.
	 */
	public record DeliveryResult(boolean success, String error, boolean retriable) {
		public static DeliveryResult ofSuccess() {
			return new DeliveryResult(true, null, false);
		}

		public static DeliveryResult ofFailure(String error, boolean retriable) {
			return new DeliveryResult(false, error, retriable);
		}
	}
}