package io.softwarestrategies.scheduledevent.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.softwarestrategies.scheduledevent.domain.EventStatus;
import io.softwarestrategies.scheduledevent.domain.ScheduledEvent;
import io.softwarestrategies.scheduledevent.dto.KafkaEventMessage;
import io.softwarestrategies.scheduledevent.repository.ScheduledEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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

	// Cache for deduplication within a short time window
	private final Set<String> recentMessageIds = ConcurrentHashMap.newKeySet();
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

		// Batch save to database
		if (!eventsToSave.isEmpty()) {
			try {
				scheduledEventRepository.saveAll(eventsToSave);
				eventsPersistedCounter.increment(eventsToSave.size());
				log.debug("Persisted {} events to database", eventsToSave.size());
			} catch (Exception e) {
				log.error("Failed to batch save events to database", e);
				// Send all to DLQ on batch failure
				for (KafkaEventMessage msg : messages) {
					kafkaProducerService.sendToDlq(msg, "Database batch save failed: " + e.getMessage());
				}
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
		// Clean up cache if too large
		if (recentMessageIds.size() > MAX_CACHE_SIZE) {
			recentMessageIds.clear();
			log.debug("Cleared message ID cache due to size limit");
		}
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
				.partitionKey(ScheduledEvent.calculatePartitionKey(message.getScheduledAt()))
				.build();
	}
}