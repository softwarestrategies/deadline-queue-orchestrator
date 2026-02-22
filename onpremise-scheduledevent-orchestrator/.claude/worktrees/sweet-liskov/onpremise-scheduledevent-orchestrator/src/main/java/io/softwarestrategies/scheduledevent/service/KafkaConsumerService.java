package io.softwarestrategies.scheduledevent.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.softwarestrategies.scheduledevent.domain.EventStatus;
import io.softwarestrategies.scheduledevent.domain.ScheduledEvent;
import io.softwarestrategies.scheduledevent.dto.KafkaEventMessage;
import io.softwarestrategies.scheduledevent.repository.ScheduledEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Kafka consumer service that handles the second phase of ingestion:
 * Kafka → Database.
 *
 * Processes messages in batches for optimal throughput.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaConsumerService {

	private final ScheduledEventRepository scheduledEventRepository;
	private final KafkaProducerService kafkaProducerService;
	private final Counter eventsPersistedCounter;
	private final Timer databaseBatchTimer;

	/**
	 * Versus simply using an LRU
	 */
	private final Set<String> recentMessageIds = Collections.newSetFromMap(
			Collections.synchronizedMap(new LinkedHashMap<>(MAX_CACHE_SIZE, 0.75f, true) {
				@Override
				protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
					return size() > MAX_CACHE_SIZE;
				}
			})
	);

	private static final int MAX_CACHE_SIZE = 100000;

	/**
	 * Batch consumer for ingestion topic.
	 * Processes messages in batches for high throughput.
	 */
	@KafkaListener(
			topics = "${app.kafka.topics.ingestion}",
			groupId = "${spring.kafka.consumer.group-id}",
			containerFactory = "kafkaListenerContainerFactory"
	)
	@Transactional
	public void consumeIngestionBatch(List<KafkaEventMessage> messages, Acknowledgment ack) {
		if (messages == null || messages.isEmpty()) {
			ack.acknowledge();
			return;
		}

		log.debug("Received batch of {} messages from ingestion topic", messages.size());

		Timer.Sample sample = Timer.start();
		List<ScheduledEvent> eventsToSave = new ArrayList<>();
		List<KafkaEventMessage> failedMessages = new ArrayList<>();

		for (KafkaEventMessage message : messages) {
			try {
				// Deduplication check
				if (isDuplicate(message)) {
					log.debug("Skipping duplicate message: {}", message.getMessageId());
					continue;
				}

				ScheduledEvent event = convertToEntity(message);
				eventsToSave.add(event);

				// Track message ID for deduplication
				trackMessageId(message.getMessageId());

			} catch (Exception e) {
				log.error("Failed to process message: {}", message.getMessageId(), e);
				failedMessages.add(message);
			}
		}

		/**
		 * There is a tradeoff here versus using a single batch insert, but with individual save() calls you can isolate
		 * failures. For the deduplication use case this is the right call since DLQ misrouting is worse than slightly
		 * lower insert throughput.
		 */
		for (ScheduledEvent event : eventsToSave) {
			try {
				scheduledEventRepository.save(event);
				eventsPersistedCounter.increment();
			} catch (DataIntegrityViolationException e) {
				// Duplicate — constraint fired. This is expected across instances.
				log.debug("Duplicate event skipped (constraint): externalJobId={}, source={}",
						event.getExternalJobId(), event.getSource());
			} catch (Exception e) {
				log.error("Failed to persist event: externalJobId={}", event.getExternalJobId(), e);
				// Find and DLQ the original message for this event
				messages.stream()
						.filter(m -> m.getExternalJobId().equals(event.getExternalJobId()))
						.findFirst()
						.ifPresent(m -> kafkaProducerService.sendToDlq(m, "Persist failed: " + e.getMessage()));
			}
		}

		// Handle failed messages
		for (KafkaEventMessage failed : failedMessages) {
			kafkaProducerService.sendToDlq(failed, "Message processing failed");
		}

		sample.stop(databaseBatchTimer);
		ack.acknowledge();
	}

	/**
	 * Check if this message has been processed recently (deduplication).
	 */
	private boolean isDuplicate(KafkaEventMessage message) {
		// Check in-memory cache
		if (recentMessageIds.contains(message.getMessageId())) {
			return true;
		}

		// Check database for exact match
		return scheduledEventRepository.existsByUniqueKey(
				message.getExternalJobId(),
				message.getSource(),
				message.getScheduledAt()
		);
	}

	/**
	 * Track message ID for deduplication.
	 */
	private void trackMessageId(String messageId) {
		// LRU eviction is handled automatically by the LinkedHashMap removeEldestEntry policy
		recentMessageIds.add(messageId);
	}

	/**
	 * Convert Kafka message to entity.
	 */
	private ScheduledEvent convertToEntity(KafkaEventMessage message) {
		return ScheduledEvent.builder()
				.externalJobId(message.getExternalJobId())
				.source(message.getSource())
				.scheduledAt(message.getScheduledAt())
				.deliveryType(message.getDeliveryType())
				.destination(message.getDestination())
				.payload(message.getPayload())
				.status(EventStatus.PENDING)
				.retryCount(0)
				.maxRetries(message.getMaxRetries() != null ? message.getMaxRetries() : 3)
				.createdAt(Instant.now())
				.updatedAt(Instant.now())
				.build();
	}
}