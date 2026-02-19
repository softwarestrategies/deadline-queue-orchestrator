package io.softwarestrategies.scheduledevent.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.softwarestrategies.scheduledevent.domain.DeliveryType;
import io.softwarestrategies.scheduledevent.domain.EventStatus;
import io.softwarestrategies.scheduledevent.domain.ScheduledEvent;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

/**
 * DTO for scheduled event responses.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ScheduledEventResponse {

	private UUID id;

	@JsonProperty("external_job_id")
	private String externalJobId;

	private String source;

	@JsonProperty("scheduled_at")
	private Instant scheduledAt;

	@JsonProperty("delivery_type")
	private DeliveryType deliveryType;

	private String destination;

	private EventStatus status;

	@JsonProperty("retry_count")
	private int retryCount;

	@JsonProperty("max_retries")
	private int maxRetries;

	@JsonProperty("created_at")
	private Instant createdAt;

	@JsonProperty("executed_at")
	private Instant executedAt;

	@JsonProperty("last_error")
	private String lastError;

	/**
	 * Convert entity to response DTO
	 */
	public static ScheduledEventResponse fromEntity(ScheduledEvent event) {
		return ScheduledEventResponse.builder()
				.id(event.getId())
				.externalJobId(event.getExternalJobId())
				.source(event.getSource())
				.scheduledAt(event.getScheduledAt())
				.deliveryType(event.getDeliveryType())
				.destination(event.getDestination())
				.status(event.getStatus())
				.retryCount(event.getRetryCount())
				.maxRetries(event.getMaxRetries())
				.createdAt(event.getCreatedAt())
				.executedAt(event.getExecutedAt())
				.lastError(event.getLastError())
				.build();
	}
}