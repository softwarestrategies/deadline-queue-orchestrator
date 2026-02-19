package io.softwarestrategies.scheduledevent.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.softwarestrategies.scheduledevent.dto.KafkaEventMessage;
import io.softwarestrategies.scheduledevent.dto.ScheduledEventRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Service for producing messages to Kafka topics.
 *
 * Handles the first phase of the two-phase ingestion: REST API → Kafka.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaProducerService {

	private final KafkaTemplate<String, Object> kafkaTemplate;
	private final ObjectMapper objectMapper;
	private final Counter eventsReceivedCounter;
	private final Timer eventIngestionTimer;

	@Value("${app.kafka.topics.ingestion}")
	private String ingestionTopic;

	@Value("${app.kafka.topics.dlq}")
	private String dlqTopic;

	/**
	 * Send a single event to the ingestion topic.
	 *
	 * @param request The event request
	 * @return The generated message ID
	 */
	public CompletableFuture<String> sendEventAsync(ScheduledEventRequest request) {
		String messageId = UUID.randomUUID().toString();

		return eventIngestionTimer.record(() -> {
			try {
				String payloadJson = objectMapper.writeValueAsString(request.getPayload());
				KafkaEventMessage message = KafkaEventMessage.fromRequest(request, messageId, payloadJson);

				// Use source as partition key for ordering guarantees per client
				String partitionKey = request.getSource() + ":" + request.getExternalJobId();

				CompletableFuture<SendResult<String, Object>> future =
						kafkaTemplate.send(ingestionTopic, partitionKey, message);

				eventsReceivedCounter.increment();

				return future.thenApply(result -> {
					log.debug("Event sent to Kafka. MessageId: {}, Partition: {}, Offset: {}",
							messageId, result.getRecordMetadata().partition(),
							result.getRecordMetadata().offset());
					return messageId;
				}).exceptionally(ex -> {
					log.error("Failed to send event to Kafka. MessageId: {}", messageId, ex);
					throw new RuntimeException("Failed to send event to Kafka", ex);
				});

			} catch (JsonProcessingException e) {
				log.error("Failed to serialize event payload. ExternalJobId: {}",
						request.getExternalJobId(), e);
				return CompletableFuture.failedFuture(e);
			}
		});
	}

	/**
	 * Send multiple events to the ingestion topic in batch.
	 *
	 * @param requests List of event requests
	 * @return List of generated message IDs
	 */
	public CompletableFuture<List<String>> sendEventsBatchAsync(List<ScheduledEventRequest> requests) {
		List<CompletableFuture<String>> futures = new ArrayList<>();

		for (ScheduledEventRequest request : requests) {
			futures.add(sendEventAsync(request));
		}

		return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
				.thenApply(v -> futures.stream()
						.map(CompletableFuture::join)
						.toList());
	}

	/**
	 * Send a message to the dead letter queue.
	 *
	 * @param message The failed message
	 * @param error   The error that caused the failure
	 */
	public void sendToDlq(KafkaEventMessage message, String error) {
		try {
			String dlqPayload = objectMapper.writeValueAsString(new DlqMessage(message, error));
			kafkaTemplate.send(dlqTopic, message.getSource(), dlqPayload);
			log.warn("Message sent to DLQ. MessageId: {}, Error: {}", message.getMessageId(), error);
		} catch (Exception e) {
			log.error("Failed to send message to DLQ. MessageId: {}", message.getMessageId(), e);
		}
	}

	/**
	 * Send event payload to external Kafka topic (for KAFKA delivery type).
	 *
	 * @param topic   Destination topic
	 * @param key     Message key
	 * @param payload Event payload
	 * @return CompletableFuture for the send result
	 */
	public CompletableFuture<Void> sendToExternalTopic(String topic, String key, String payload) {
		return kafkaTemplate.send(topic, key, payload)
				.thenAccept(result -> log.debug("Event delivered to external topic. Topic: {}, Partition: {}",
						topic, result.getRecordMetadata().partition()))
				.exceptionally(ex -> {
					log.error("Failed to deliver event to external topic: {}", topic, ex);
					throw new RuntimeException("Failed to deliver to external topic", ex);
				});
	}

	/**
	 * DLQ message wrapper.
	 */
	private record DlqMessage(KafkaEventMessage originalMessage, String error) {}
}