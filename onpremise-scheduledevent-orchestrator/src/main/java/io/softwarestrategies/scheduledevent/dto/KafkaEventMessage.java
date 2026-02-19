package io.softwarestrategies.scheduledevent.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.softwarestrategies.scheduledevent.domain.DeliveryType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Message DTO for Kafka ingestion pipeline.
 *
 * This represents the event as it flows through the Kafka ingestion topic
 * before being persisted to the database.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class KafkaEventMessage {

	/**
	 * Unique message ID for idempotency
	 */
	@JsonProperty("message_id")
	private String messageId;

	/**
	 * External job identifier from the calling system
	 */
	@JsonProperty("external_job_id")
	private String externalJobId;

	/**
	 * Source system identifier
	 */
	private String source;

	/**
	 * When the event should be executed
	 */
	@JsonProperty("scheduled_at")
	private Instant scheduledAt;

	/**
	 * Delivery type: HTTP or KAFKA
	 */
	@JsonProperty("delivery_type")
	private DeliveryType deliveryType;

	/**
	 * Destination URL (for HTTP) or topic name (for KAFKA)
	 */
	private String destination;

	/**
	 * The payload to deliver as JSON string
	 */
	private String payload;

	/**
	 * Maximum retry attempts
	 */
	@JsonProperty("max_retries")
	private Integer maxRetries;

	/**
	 * Timestamp when message was received
	 */
	@JsonProperty("received_at")
	private Instant receivedAt;

	/**
	 * Create from request DTO
	 */
	public static KafkaEventMessage fromRequest(ScheduledEventRequest request, String messageId, String payloadJson) {
		return KafkaEventMessage.builder()
				.messageId(messageId)
				.externalJobId(request.getExternalJobId())
				.source(request.getSource())
				.scheduledAt(request.getScheduledAt())
				.deliveryType(request.getDeliveryType())
				.destination(request.getDestination())
				.payload(payloadJson)
				.maxRetries(request.getMaxRetries())
				.receivedAt(Instant.now())
				.build();
	}
}