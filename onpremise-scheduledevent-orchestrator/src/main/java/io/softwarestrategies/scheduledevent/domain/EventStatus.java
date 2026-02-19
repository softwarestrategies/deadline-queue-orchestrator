package io.softwarestrategies.scheduledevent.domain;

/**
 * Defines the possible states of a scheduled event throughout its lifecycle.
 */
public enum EventStatus {
	/**
	 * Event has been received and persisted, awaiting execution
	 */
	PENDING,

	/**
	 * Event has been picked up by a worker and is being processed
	 */
	PROCESSING,

	/**
	 * Event was successfully delivered to the destination
	 */
	COMPLETED,

	/**
	 * Event delivery failed and will be retried
	 */
	FAILED,

	/**
	 * Event delivery failed after all retry attempts
	 */
	DEAD_LETTER,

	/**
	 * Event was cancelled before execution
	 */
	CANCELLED
}