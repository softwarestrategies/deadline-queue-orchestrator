package io.softwarestrategies.scheduledevent.service;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.softwarestrategies.scheduledevent.domain.ScheduledEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

/**
 * Service responsible for delivering scheduled events to their destinations.
 * Supports both HTTP POST and Kafka topic delivery.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class EventDeliveryService {

	private final WebClient webClient;
	private final KafkaProducerService kafkaProducerService;
	private final Counter httpDeliveryCounter;
	private final Counter kafkaDeliveryCounter;
	private final Timer eventDeliveryTimer;

	// HTTP status codes that indicate retriable errors
	private static final int[] RETRIABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504};

	/**
	 * Deliver an event to its destination.
	 *
	 * @param event The event to deliver
	 * @return CompletableFuture that completes when delivery is done
	 */
	public CompletableFuture<DeliveryResult> deliverEvent(ScheduledEvent event) {
		Timer.Sample sample = Timer.start();

		CompletableFuture<DeliveryResult> result = switch (event.getDeliveryType()) {
			case HTTP -> deliverViaHttp(event);
			case KAFKA -> deliverViaKafka(event);
		};

		return result.whenComplete((r, ex) -> sample.stop(eventDeliveryTimer));
	}

	/**
	 * Deliver event via HTTP POST.
	 */
	private CompletableFuture<DeliveryResult> deliverViaHttp(ScheduledEvent event) {
		httpDeliveryCounter.increment();

		return webClient.post()
				.uri(event.getDestination())
				.contentType(MediaType.APPLICATION_JSON)
				.bodyValue(event.getPayload())
				.retrieve()
				.toBodilessEntity()
				.retryWhen(Retry.backoff(2, Duration.ofMillis(500))
						.filter(this::isRetriableHttpError)
						.onRetryExhaustedThrow((spec, signal) -> signal.failure()))
				.map(response -> {
					log.debug("HTTP delivery successful. EventId: {}, Status: {}",
							event.getId(), response.getStatusCode());
					return DeliveryResult.ofSuccess();
				})
				.onErrorResume(ex -> {
					String error = extractErrorMessage(ex);
					boolean retriable = isRetriableHttpError(ex);
					log.warn("HTTP delivery failed. EventId: {}, Retriable: {}, Error: {}",
							event.getId(), retriable, error);
					return Mono.just(DeliveryResult.ofFailure(error, retriable));
				})
				.toFuture();
	}

	/**
	 * Deliver event via Kafka topic.
	 */
	private CompletableFuture<DeliveryResult> deliverViaKafka(ScheduledEvent event) {
		kafkaDeliveryCounter.increment();

		return kafkaProducerService.sendToExternalTopic(
				event.getDestination(),
				event.getExternalJobId(),
				event.getPayload()
		).thenApply(v -> {
			log.debug("Kafka delivery successful. EventId: {}, Topic: {}",
					event.getId(), event.getDestination());
			return DeliveryResult.ofSuccess();
		}).exceptionally(ex -> {
			String error = "Kafka delivery failed: " + ex.getMessage();
			log.warn("Kafka delivery failed. EventId: {}, Error: {}", event.getId(), error);
			return DeliveryResult.ofFailure(error, true); // Kafka errors are generally retriable
		});
	}

	/**
	 * Check if an HTTP error is retriable.
	 */
	private boolean isRetriableHttpError(Throwable ex) {
		if (ex instanceof WebClientResponseException wcEx) {
			int statusCode = wcEx.getStatusCode().value();
			for (int retriable : RETRIABLE_STATUS_CODES) {
				if (statusCode == retriable) {
					return true;
				}
			}
			return false;
		}
		// Network errors are retriable
		return ex instanceof java.net.ConnectException ||
				ex instanceof java.net.SocketTimeoutException ||
				ex instanceof io.netty.channel.ConnectTimeoutException;
	}

	/**
	 * Extract error message from exception.
	 */
	private String extractErrorMessage(Throwable ex) {
		if (ex instanceof WebClientResponseException wcEx) {
			return String.format("HTTP %d: %s", wcEx.getStatusCode().value(),
					wcEx.getStatusText());
		}
		return ex.getMessage() != null ? ex.getMessage() : ex.getClass().getSimpleName();
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