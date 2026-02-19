package io.softwarestrategies.scheduledevent.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.softwarestrategies.scheduledevent.domain.DeliveryType;
import jakarta.validation.constraints.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * DTO for scheduled event creation requests.
 *
 * This is the payload accepted by the REST API endpoint.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ScheduledEventRequest {

	/**
	 * External job identifier from the calling system
	 */
	@NotBlank(message = "External job ID is required")
	@Size(max = 255, message = "External job ID must not exceed 255 characters")
	@JsonProperty("external_job_id")
	private String externalJobId;

	/**
	 * Source system identifier
	 */
	@NotBlank(message = "Source is required")
	@Size(max = 100, message = "Source must not exceed 100 characters")
	private String source;

	/**
	 * When the event should be executed
	 */
	@NotNull(message = "Scheduled time is required")
	@Future(message = "Scheduled time must be in the future")
	@JsonProperty("scheduled_at")
	private Instant scheduledAt;

	/**
	 * Delivery type: HTTP or KAFKA
	 */
	@NotNull(message = "Delivery type is required")
	@JsonProperty("delivery_type")
	private DeliveryType deliveryType;

	/**
	 * Destination URL (for HTTP) or topic name (for KAFKA)
	 */
	@NotBlank(message = "Destination is required")
	@Size(max = 2048, message = "Destination must not exceed 2048 characters")
	private String destination;

	/**
	 * The payload to deliver - must be valid JSON
	 */
	@NotNull(message = "Payload is required")
	private Object payload;

	/**
	 * Maximum retry attempts (optional, defaults to 3)
	 */
	@Min(value = 0, message = "Max retries must be non-negative")
	@Max(value = 10, message = "Max retries must not exceed 10")
	@JsonProperty("max_retries")
	@Builder.Default
	private Integer maxRetries = 3;
}