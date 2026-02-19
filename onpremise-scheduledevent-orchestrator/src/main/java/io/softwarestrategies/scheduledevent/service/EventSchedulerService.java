package io.softwarestrategies.scheduledevent.service;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import io.micrometer.core.instrument.Counter;
import io.softwarestrategies.scheduledevent.domain.ScheduledEvent;
import io.softwarestrategies.scheduledevent.repository.ScheduledEventRepository;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Service responsible for polling and executing scheduled events.
 *
 * Uses SELECT FOR UPDATE SKIP LOCKED for distributed processing across
 * multiple instances without conflicts.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class EventSchedulerService {

	private final ScheduledEventRepository scheduledEventRepository;
	private final EventDeliveryService eventDeliveryService;
	private final EventPersistenceService eventPersistenceService;
	private final Counter eventsExecutedCounter;
	private final Counter eventsFailedCounter;

	private final AtomicBoolean running = new AtomicBoolean(true);

	@PreDestroy
	public void shutdown() {
		running.set(false);
		log.info("Event scheduler shutting down...");
	}

	/**
	 * Main polling loop that fetches and processes due events.
	 * Runs at configured interval using virtual threads for execution.
	 */
	@Scheduled(fixedDelayString = "${app.scheduler.poll-interval-ms:1000}")
	public void pollAndExecuteEvents() {
		if (!running.get()) {
			return;
		}

		try {
			List<ScheduledEvent> events = eventPersistenceService.fetchDueEvents();

			if (events.isEmpty()) {
				return;
			}

			log.debug("Fetched {} events for execution", events.size());

			for (ScheduledEvent event : events) {
				processEvent(event);
			}

		} catch (Exception e) {
			log.error("Error in scheduler poll loop", e);
		}
	}

	/**
	 * Process a single event - deliver and update status.
	 */
	private void processEvent(ScheduledEvent event) {
		try {
			log.debug("Processing event. Id: {}, ExternalJobId: {}", event.getId(), event.getExternalJobId());

			// Check if event should be executed now
			if (event.getScheduledAt().isAfter(Instant.now())) {
				// Schedule for later - this shouldn't happen often with proper polling
				eventPersistenceService.rescheduleEvent(event);
				return;
			}

			// Deliver the event
			EventDeliveryService.DeliveryResult result = eventDeliveryService.deliverEvent(event);

			if (result.success()) {
				eventPersistenceService.markEventCompleted(event);
				eventsExecutedCounter.increment();
			} else {
				eventPersistenceService.handleDeliveryFailure(event, result);
			}

		} catch (Exception e) {
			log.error("Unexpected error processing event: {}", event.getId(), e);
			eventPersistenceService.handleDeliveryFailure(event, EventDeliveryService.DeliveryResult.ofFailure(
					e.getMessage(), true));
			eventsFailedCounter.increment();
		}
	}

	/**
	 * Release expired locks from crashed workers.
	 * Runs periodically to recover stuck events.
	 */
	@Scheduled(fixedRate = 60000) // Every minute
	@Transactional
	public void releaseExpiredLocks() {
		Instant now = Instant.now();
		int released = scheduledEventRepository.releaseExpiredLocks(now);
		if (released > 0) {
			log.info("Released {} expired locks", released);
		}
	}
}