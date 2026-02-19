package io.softwarestrategies.scheduledevent.service;

import java.net.InetAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import io.micrometer.core.instrument.Counter;
import io.softwarestrategies.scheduledevent.domain.EventStatus;
import io.softwarestrategies.scheduledevent.domain.ScheduledEvent;
import io.softwarestrategies.scheduledevent.repository.ScheduledEventRepository;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Slf4j
public class EventPersistenceService {

	private final ScheduledEventRepository scheduledEventRepository;
	private final Counter eventsFailedCounter;

	@Value("${app.scheduler.batch-size:100}")
	private int batchSize;

	private String workerId;

	@PostConstruct
	public void init() {
		try {
			workerId = InetAddress.getLocalHost().getHostName() + "-" + UUID.randomUUID().toString().substring(0, 8);
		} catch (Exception e) {
			workerId = "worker-" + UUID.randomUUID().toString().substring(0, 8);
		}
		log.info("Event scheduler initialized with worker ID: {}", workerId);
	}

	/**
	 * Fetch events that are due for execution using SELECT FOR UPDATE SKIP LOCKED.
	 */
	@Transactional
	public List<ScheduledEvent> fetchDueEvents() {
		Instant now = Instant.now();

		log.debug("Polling for due events. Now: {}, BatchSize: {}", now, batchSize);

		List<ScheduledEvent> events = scheduledEventRepository.findAndLockEventsForProcessing(
				EventStatus.PENDING.name(),
				now,
				now,
				batchSize
		);

		log.debug("Found {} events ready for execution", events.size());

		// Acquire locks on fetched events
		Instant lockExpiry = now.plus(Duration.ofMinutes(5));
		for (ScheduledEvent event : events) {
			event.acquireLock(workerId, lockExpiry);
		}

		if (!events.isEmpty()) {
			scheduledEventRepository.saveAll(events);
		}

		return events;
	}

	/**
	 * Reschedule an event for later execution.
	 */
	@Transactional
	public void rescheduleEvent(ScheduledEvent event) {
		event.setStatus(EventStatus.PENDING);
		event.setLockedBy(null);
		event.setLockExpiresAt(null);
		scheduledEventRepository.save(event);
		log.debug("Event rescheduled. Id: {}, ScheduledAt: {}", event.getId(), event.getScheduledAt());
	}

	/**
	 * Mark event as completed.
	 */
	@Transactional
	public void markEventCompleted(ScheduledEvent event) {
		event.markCompleted();
		scheduledEventRepository.save(event);
		log.debug("Event completed. Id: {}, ExternalJobId: {}", event.getId(), event.getExternalJobId());
	}

	/**
	 * Handle delivery failure - retry or dead letter.
	 */
	@Transactional
	public void handleDeliveryFailure(ScheduledEvent event, EventDeliveryService.DeliveryResult result) {
		event.markFailed(result.error());
		scheduledEventRepository.save(event);

		if (event.getStatus() == EventStatus.DEAD_LETTER) {
			log.warn("Event moved to dead letter. Id: {}, ExternalJobId: {}, Error: {}",
					event.getId(), event.getExternalJobId(), result.error());
		} else {
			log.debug("Event will be retried. Id: {}, RetryCount: {}/{}",
					event.getId(), event.getRetryCount(), event.getMaxRetries());
		}

		eventsFailedCounter.increment();
	}
}