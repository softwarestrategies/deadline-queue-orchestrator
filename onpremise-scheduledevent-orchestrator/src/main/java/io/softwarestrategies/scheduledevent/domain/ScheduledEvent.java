package io.softwarestrategies.scheduledevent.domain;

import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * Entity representing a scheduled event to be delivered at a specific time.
 *
 * The table is partitioned by scheduled_at using RANGE partitioning for efficient
 * querying of events due for execution.
 */
@Entity
@Table(name = "scheduled_events", indexes = {
		@Index(name = "idx_scheduled_events_status_scheduled_at", columnList = "status, scheduled_at"),
		@Index(name = "idx_scheduled_events_external_job_id", columnList = "external_job_id"),
		@Index(name = "idx_scheduled_events_source", columnList = "source")
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
@EqualsAndHashCode(of = "id")
@ToString(exclude = "payload")
public class ScheduledEvent {

	@Id
	@GeneratedValue(strategy = GenerationType.UUID)
	@Column(name = "id", nullable = false, updatable = false)
	private UUID id;

	/**
	 * External job identifier provided by the client
	 */
	@Column(name = "external_job_id", nullable = false, length = 255)
	private String externalJobId;

	/**
	 * Source system that submitted the event
	 */
	@Column(name = "source", nullable = false, length = 100)
	private String source;

	/**
	 * When the event should be executed/delivered
	 */
	@Column(name = "scheduled_at", nullable = false)
	private Instant scheduledAt;

	/**
	 * How the event should be delivered (HTTP or KAFKA)
	 */
	@Enumerated(EnumType.STRING)
	@Column(name = "delivery_type", nullable = false, length = 20)
	private DeliveryType deliveryType;

	/**
	 * Destination for delivery - URL for HTTP, topic name for KAFKA
	 */
	@Column(name = "destination", nullable = false, length = 2048)
	private String destination;

	/**
	 * The JSON payload to be delivered
	 */
	@JdbcTypeCode(SqlTypes.JSON)
	@Column(name = "payload", nullable = false, columnDefinition = "jsonb")
	private String payload;

	/**
	 * Current status of the event
	 */
	@Enumerated(EnumType.STRING)
	@Column(name = "status", nullable = false, length = 20)
	@Builder.Default
	private EventStatus status = EventStatus.PENDING;

	/**
	 * Number of delivery attempts made
	 */
	@Column(name = "retry_count", nullable = false)
	@Builder.Default
	private int retryCount = 0;

	/**
	 * Maximum number of retry attempts allowed
	 */
	@Column(name = "max_retries", nullable = false)
	@Builder.Default
	private int maxRetries = 3;

	/**
	 * Last error message if delivery failed
	 */
	@Column(name = "last_error", length = 4000)
	private String lastError;

	/**
	 * When the event was created
	 */
	@Column(name = "created_at", nullable = false, updatable = false)
	@Builder.Default
	private Instant createdAt = Instant.now();

	/**
	 * When the event was last updated
	 */
	@Column(name = "updated_at", nullable = false)
	@Builder.Default
	private Instant updatedAt = Instant.now();

	/**
	 * When the event was executed (null if not yet executed)
	 */
	@Column(name = "executed_at")
	private Instant executedAt;

	/**
	 * Instance ID of the worker that is processing this event
	 */
	@Column(name = "locked_by", length = 100)
	private String lockedBy;

	/**
	 * When the lock expires (for stale lock detection)
	 */
	@Column(name = "lock_expires_at")
	private Instant lockExpiresAt;

	/**
	 * Partition key derived from scheduled_at for table partitioning
	 */
	@Column(name = "partition_key", nullable = false)
	private int partitionKey;

	@PrePersist
	protected void onCreate() {
		if (createdAt == null) {
			createdAt = Instant.now();
		}
		updatedAt = Instant.now();
		partitionKey = calculatePartitionKey(scheduledAt);
	}

	@PreUpdate
	protected void onUpdate() {
		updatedAt = Instant.now();
	}

	/**
	 * Calculate partition key from scheduled time.
	 * Uses day-of-year for daily partitions.
	 */
	public static int calculatePartitionKey(Instant scheduledAt) {
		return scheduledAt.atZone(java.time.ZoneOffset.UTC).getDayOfYear()
				+ (scheduledAt.atZone(java.time.ZoneOffset.UTC).getYear() * 1000);
	}

	/**
	 * Check if this event can be retried
	 */
	public boolean canRetry() {
		return retryCount < maxRetries;
	}

	/**
	 * Increment retry count and return the new value
	 */
	public int incrementRetryCount() {
		return ++retryCount;
	}

	/**
	 * Mark the event as completed
	 */
	public void markCompleted() {
		this.status = EventStatus.COMPLETED;
		this.executedAt = Instant.now();
		this.lockedBy = null;
		this.lockExpiresAt = null;
	}

	/**
	 * Mark the event as failed
	 */
	public void markFailed(String error) {
		this.lastError = error != null ? error.substring(0, Math.min(error.length(), 4000)) : null;
		this.retryCount++;

		if (canRetry()) {
			this.status = EventStatus.PENDING;
			this.lockedBy = null;
			this.lockExpiresAt = null;
		} else {
			this.status = EventStatus.DEAD_LETTER;
			this.executedAt = Instant.now();
		}
	}

	/**
	 * Acquire a lock for processing
	 */
	public void acquireLock(String workerId, Instant expiresAt) {
		this.status = EventStatus.PROCESSING;
		this.lockedBy = workerId;
		this.lockExpiresAt = expiresAt;
	}
}