package io.softwarestrategies.scheduledevent.controller;

import io.micrometer.core.annotation.Timed;
import io.softwarestrategies.scheduledevent.dto.*;
import io.softwarestrategies.scheduledevent.service.EventCleanupService;
import io.softwarestrategies.scheduledevent.service.ScheduledEventService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

/**
 * REST controller for scheduled event operations.
 *
 * Provides endpoints for:
 * - Submitting scheduled events (single and batch)
 * - Querying event status
 * - Cancelling events
 * - System statistics
 */
@RestController
@RequestMapping("/api/v1/events")
@RequiredArgsConstructor
@Slf4j
public class ScheduledEventController {

	private final ScheduledEventService scheduledEventService;
	private final EventCleanupService eventCleanupService;

	/**
	 * Submit a single scheduled event.
	 *
	 * The event is immediately queued to Kafka for asynchronous processing,
	 * providing backpressure handling for thundering herd scenarios.
	 */
	@PostMapping
	@Timed(value = "scheduledevent.api.submit", description = "Time to submit a single event")
	public ResponseEntity<ApiResponse<SubmitEventResponse>> submitEvent(
			@Valid @RequestBody ScheduledEventRequest request) {

		log.debug("Received event submission. ExternalJobId: {}, Source: {}, ScheduledAt: {}",
				request.getExternalJobId(), request.getSource(), request.getScheduledAt());

		String messageId = scheduledEventService.submitEvent(request).join();
		SubmitEventResponse response = new SubmitEventResponse(messageId, "Event queued for processing");
		return ResponseEntity.status(HttpStatus.ACCEPTED)
				.body(ApiResponse.success(response));
	}

	/**
	 * Submit a batch of scheduled events.
	 *
	 * Allows submitting up to 1000 events in a single request.
	 */
	@PostMapping("/batch")
	@Timed(value = "scheduledevent.api.submit.batch", description = "Time to submit a batch of events")
	public ResponseEntity<ApiResponse<BatchScheduledEventResponse>> submitEventsBatch(
			@Valid @RequestBody BatchScheduledEventRequest request) {

		log.info("Received batch submission of {} events", request.getEvents().size());

		BatchScheduledEventResponse response = scheduledEventService.submitEventsBatch(request).join();
		return ResponseEntity.status(HttpStatus.ACCEPTED)
				.body(ApiResponse.success(response));
	}

	/**
	 * Get event by internal ID.
	 */
	@GetMapping("/{id}")
	@Timed(value = "scheduledevent.api.get", description = "Time to get an event by ID")
	public ResponseEntity<ApiResponse<ScheduledEventResponse>> getEventById(@PathVariable UUID id) {
		ScheduledEventResponse event = scheduledEventService.getEventById(id);
		return ResponseEntity.ok(ApiResponse.success(event));
	}

	/**
	 * Get event by external job ID.
	 */
	@GetMapping("/external/{externalJobId}")
	@Timed(value = "scheduledevent.api.get.external", description = "Time to get an event by external job ID")
	public ResponseEntity<ApiResponse<ScheduledEventResponse>> getEventByExternalJobId(
			@PathVariable String externalJobId) {
		ScheduledEventResponse event = scheduledEventService.getEventByExternalJobId(externalJobId);
		return ResponseEntity.ok(ApiResponse.success(event));
	}

	/**
	 * Get all events for a given external job ID.
	 */
	@GetMapping("/external/{externalJobId}/all")
	public ResponseEntity<ApiResponse<List<ScheduledEventResponse>>> getAllEventsByExternalJobId(
			@PathVariable String externalJobId) {
		List<ScheduledEventResponse> events = scheduledEventService.getEventsByExternalJobId(externalJobId);
		return ResponseEntity.ok(ApiResponse.success(events));
	}

	/**
	 * Cancel an event by internal ID.
	 * Only pending events can be cancelled.
	 */
	@DeleteMapping("/{id}")
	@Timed(value = "scheduledevent.api.cancel", description = "Time to cancel an event")
	public ResponseEntity<ApiResponse<Void>> cancelEventById(@PathVariable UUID id) {
		boolean cancelled = scheduledEventService.cancelEventById(id);
		if (cancelled) {
			return ResponseEntity.ok(ApiResponse.success(null, "Event cancelled"));
		}
		return ResponseEntity.status(HttpStatus.NOT_FOUND)
				.body(ApiResponse.error("Event not found or cannot be cancelled", "NOT_FOUND"));
	}

	/**
	 * Cancel event(s) by external job ID.
	 */
	@DeleteMapping("/external/{externalJobId}")
	public ResponseEntity<ApiResponse<Void>> cancelEventByExternalJobId(@PathVariable String externalJobId) {
		boolean cancelled = scheduledEventService.cancelEvent(externalJobId);
		if (cancelled) {
			return ResponseEntity.ok(ApiResponse.success(null, "Event(s) cancelled"));
		}
		return ResponseEntity.status(HttpStatus.NOT_FOUND)
				.body(ApiResponse.error("No pending events found for external job ID", "NOT_FOUND"));
	}

	/**
	 * Get event statistics.
	 */
	@GetMapping("/statistics")
	public ResponseEntity<ApiResponse<ScheduledEventService.EventStatistics>> getStatistics() {
		ScheduledEventService.EventStatistics stats = scheduledEventService.getStatistics();
		return ResponseEntity.ok(ApiResponse.success(stats));
	}

	/**
	 * Trigger manual cleanup (admin operation).
	 */
	@PostMapping("/admin/cleanup")
	public ResponseEntity<ApiResponse<EventCleanupService.CleanupResult>> triggerCleanup(
			@RequestParam(defaultValue = "7") int days) {
		EventCleanupService.CleanupResult result = eventCleanupService.manualCleanup(days);
		return ResponseEntity.ok(ApiResponse.success(result, "Cleanup completed"));
	}

	/**
	 * Health check endpoint.
	 */
	@GetMapping("/health")
	public ResponseEntity<ApiResponse<String>> health() {
		return ResponseEntity.ok(ApiResponse.success("OK"));
	}

	/**
	 * Response DTO for single event submission.
	 */
	public record SubmitEventResponse(String messageId, String message) {}
}