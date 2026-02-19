package io.softwarestrategies.scheduledevent.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.softwarestrategies.scheduledevent.domain.EventStatus;
import io.softwarestrategies.scheduledevent.domain.ScheduledEvent;
import io.softwarestrategies.scheduledevent.dto.*;
import io.softwarestrategies.scheduledevent.exception.EventNotFoundException;
import io.softwarestrategies.scheduledevent.exception.ScheduledEventException;
import io.softwarestrategies.scheduledevent.repository.ScheduledEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Main service for scheduled event operations.
 * Handles event submission, querying, and cancellation.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class ScheduledEventService {

	private final KafkaProducerService kafkaProducerService;
	private final ScheduledEventRepository scheduledEventRepository;
	private final ObjectMapper objectMapper;

	/**
	 * Submit a single scheduled event.
	 * Uses async Kafka ingestion for thundering herd protection.
	 *
	 * @param request The event request
	 * @return The generated message ID
	 */
	public CompletableFuture<String> submitEvent(ScheduledEventRequest request) {
		validateRequest(request);
		return kafkaProducerService.sendEventAsync(request);
	}

	/**
	 * Submit a batch of scheduled events.
	 *
	 * @param batchRequest The batch request
	 * @return Batch submission response
	 */
	public CompletableFuture<BatchScheduledEventResponse> submitEventsBatch(BatchScheduledEventRequest batchRequest) {
		List<String> acceptedIds = new ArrayList<>();
		List<BatchScheduledEventResponse.RejectedEvent> rejectedEvents = new ArrayList<>();
		List<CompletableFuture<String>> futures = new ArrayList<>();

		for (int i = 0; i < batchRequest.getEvents().size(); i++) {
			ScheduledEventRequest request = batchRequest.getEvents().get(i);
			try {
				validateRequest(request);
				futures.add(kafkaProducerService.sendEventAsync(request));
			} catch (Exception e) {
				rejectedEvents.add(BatchScheduledEventResponse.RejectedEvent.builder()
						.index(i)
						.externalJobId(request.getExternalJobId())
						.reason(e.getMessage())
						.build());
			}
		}

		return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
				.thenApply(v -> {
					for (CompletableFuture<String> future : futures) {
						try {
							acceptedIds.add(future.join());
						} catch (Exception e) {
							// Already handled above
						}
					}

					return BatchScheduledEventResponse.builder()
							.totalSubmitted(batchRequest.getEvents().size())
							.totalAccepted(acceptedIds.size())
							.totalRejected(rejectedEvents.size())
							.acceptedMessageIds(acceptedIds)
							.rejectedEvents(rejectedEvents)
							.build();
				});
	}

	/**
	 * Get an event by ID.
	 */
	@Transactional(readOnly = true)
	public ScheduledEventResponse getEventById(UUID id) {
		ScheduledEvent event = scheduledEventRepository.findById(id)
				.orElseThrow(() -> new EventNotFoundException(id.toString()));
		return ScheduledEventResponse.fromEntity(event);
	}

	/**
	 * Get an event by external job ID.
	 */
	@Transactional(readOnly = true)
	public ScheduledEventResponse getEventByExternalJobId(String externalJobId) {
		ScheduledEvent event = scheduledEventRepository.findByExternalJobId(externalJobId)
				.orElseThrow(() -> new EventNotFoundException(externalJobId));
		return ScheduledEventResponse.fromEntity(event);
	}

	/**
	 * Get all events for a given external job ID.
	 */
	@Transactional(readOnly = true)
	public List<ScheduledEventResponse> getEventsByExternalJobId(String externalJobId) {
		return scheduledEventRepository.findAllByExternalJobId(externalJobId).stream()
				.map(ScheduledEventResponse::fromEntity)
				.toList();
	}

	/**
	 * Cancel an event by external job ID.
	 * Only pending events can be cancelled.
	 */
	@Transactional
	public boolean cancelEvent(String externalJobId) {
		int cancelled = scheduledEventRepository.cancelByExternalJobId(
				externalJobId,
				Instant.now(),
				EventStatus.CANCELLED,
				EventStatus.PENDING);
		if (cancelled > 0) {
			log.info("Cancelled {} event(s) for external job ID: {}", cancelled, externalJobId);
			return true;
		}
		return false;
	}

	/**
	 * Cancel an event by internal ID.
	 */
	@Transactional
	public boolean cancelEventById(UUID id) {
		Optional<ScheduledEvent> eventOpt = scheduledEventRepository.findById(id);
		if (eventOpt.isEmpty()) {
			throw new EventNotFoundException(id.toString());
		}

		ScheduledEvent event = eventOpt.get();
		if (event.getStatus() != EventStatus.PENDING) {
			throw new ScheduledEventException(
					"Cannot cancel event in status: " + event.getStatus(),
					"INVALID_STATE");
		}

		event.setStatus(EventStatus.CANCELLED);
		event.setUpdatedAt(Instant.now());
		scheduledEventRepository.save(event);

		log.info("Cancelled event: {}", id);
		return true;
	}

	/**
	 * Get event statistics by status.
	 */
	@Transactional(readOnly = true)
	public EventStatistics getStatistics() {
		List<Object[]> stats = scheduledEventRepository.getStatusStatistics();

		EventStatistics statistics = new EventStatistics();
		for (Object[] row : stats) {
			String status = (String) row[0];
			long count = ((Number) row[1]).longValue();

			switch (EventStatus.valueOf(status)) {
				case PENDING -> statistics.setPending(count);
				case PROCESSING -> statistics.setProcessing(count);
				case COMPLETED -> statistics.setCompleted(count);
				case FAILED -> statistics.setFailed(count);
				case DEAD_LETTER -> statistics.setDeadLetter(count);
				case CANCELLED -> statistics.setCancelled(count);
			}
		}

		return statistics;
	}

	/**
	 * Validate event request.
	 */
	private void validateRequest(ScheduledEventRequest request) {
		if (request.getScheduledAt().isBefore(Instant.now())) {
			throw new ScheduledEventException("Scheduled time must be in the future", "INVALID_SCHEDULE_TIME");
		}

		// Validate payload is valid JSON
		if (request.getPayload() != null) {
			try {
				objectMapper.writeValueAsString(request.getPayload());
			} catch (JsonProcessingException e) {
				throw new ScheduledEventException("Invalid payload JSON", "INVALID_PAYLOAD");
			}
		}

		// Validate destination format based on delivery type
		switch (request.getDeliveryType()) {
			case HTTP -> {
				if (!request.getDestination().startsWith("http://") &&
						!request.getDestination().startsWith("https://")) {
					throw new ScheduledEventException(
							"HTTP destination must be a valid URL", "INVALID_DESTINATION");
				}
			}
			case KAFKA -> {
				if (request.getDestination().contains(" ")) {
					throw new ScheduledEventException(
							"Kafka topic name cannot contain spaces", "INVALID_DESTINATION");
				}
			}
		}
	}

	/**
	 * Event statistics DTO.
	 */
	@lombok.Data
	@lombok.NoArgsConstructor
	@lombok.AllArgsConstructor
	public static class EventStatistics {
		private long pending;
		private long processing;
		private long completed;
		private long failed;
		private long deadLetter;
		private long cancelled;

		public long getTotal() {
			return pending + processing + completed + failed + deadLetter + cancelled;
		}
	}
}