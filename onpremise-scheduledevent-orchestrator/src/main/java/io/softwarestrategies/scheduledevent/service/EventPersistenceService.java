package io.softwarestrategies.scheduledevent.service;

import java.net.InetAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

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

	@Value("${app.scheduler.poll-interval-ms:1000}")
	private long pollIntervalMs;

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
}