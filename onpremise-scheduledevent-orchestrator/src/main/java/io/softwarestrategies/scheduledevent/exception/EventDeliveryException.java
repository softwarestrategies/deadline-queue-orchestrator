package io.softwarestrategies.scheduledevent.exception;

import lombok.Getter;

/**
 * Exception thrown when event delivery fails.
 */
@Getter
public class EventDeliveryException extends ScheduledEventException {

	private final boolean retryable;

	public EventDeliveryException(String message, boolean retryable) {
		super(message, "DELIVERY_FAILED");
		this.retryable = retryable;
	}

	public EventDeliveryException(String message, boolean retryable, Throwable cause) {
		super(message, "DELIVERY_FAILED", cause);
		this.retryable = retryable;
	}

}